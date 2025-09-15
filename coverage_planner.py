#!/usr/bin/env python3
"""
AIpowerCoin — Coverage Planner (EU)

Calcola:
1) S (supply del giorno) = S_base * U_hat (da meter_out.csv)
2) StockValue_€ = Energia (ENTSO-E Wh_DC -> MWh * €/MWh) + Compute (unità * €/unit)
3) €/AICP implicito = StockValue_€ / S
4) Coverage Ratio CR = StockValue_€ / (S * eur_per_AICP_target)
5) Se CR < target_CR: suggerisce piano acquisti (cheapest-first) per colmare il gap

Input richiesti:
- data/grid_load.csv con colonna: date, grid_wh_dc (Wh)
- meter_out.csv con colonna: date, U_hat (usa ultima riga)
- config in testa (S_base, eur_per_AICP_target, target_CR, ENERGY_EUR_PER_MWH, ASSETS)

Esegui:
  python coverage_planner.py
  (opzioni con --help)
"""
import os, sys, argparse, math, datetime as dt
import pandas as pd

# ------------------ CONFIG (modifica qui se serve) ------------------

S_BASE = 1_000_000               # AICP base/giorno
EUR_PER_AICP_TARGET = 1.50       # target €/AICP policy giornaliera
TARGET_CR = 1.10                 # coverage ratio desiderato (>=1.0)

# Energia (ENTSO-E) — prezzo medio day-ahead stimato
ENERGY_EUR_PER_MWH = 90.0        # €/MWh
ALLOW_BUY_ENERGY   = True         # se True, può comprare MWh per coprire gap

# Compute assets disponibili (impegnati oggi + capacità extra acquistabile)
# unità: scegli tu un'unità canonica (es. H100_sec_offpeak = secondi H100 off-peak)
ASSETS = [
    {
        "name": "GPU_H100_sec_offpeak",
        "price_eur_per_unit": 0.65,         # €/secondo H100 off-peak (demo)
        "committed_units_today":  50_000,   # stock già impegnato (sec)
        "max_extra_units":        150_000,  # massimi sec acquistabili oggi
    },
    # Aggiungi altre righe se hai più risorse
]

# Mix preferito per copertura (peso relativo se vuoi forzare una quota)
# Se None: strategy cheapest-first pura
PREFERRED_MIX = None  # es. {"energy": 0.3, "GPU_H100_sec_offpeak": 0.7}

# ------------------ PATHS ------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "data")
GRID_CSV   = os.path.join(DATA_DIR, "grid_load.csv")
METER_CSV  = os.path.join(SCRIPT_DIR, "meter_out.csv")

# ------------------ UTILS ------------------

def read_grid_today(grid_csv: str) -> float:
    df = pd.read_csv(grid_csv)
    if "date" not in df.columns or "grid_wh_dc" not in df.columns:
        raise SystemExit("grid_load.csv deve avere colonne: date, grid_wh_dc")
    today = dt.date.today().isoformat()
    row = df[df["date"] == today]
    if row.empty:
        # usa ultima riga disponibile (warning) per demo
        row = df.tail(1)
        print(f"WARN: grid_load.csv non ha la data di oggi; uso ultima riga {row['date'].iloc[0]}")
    wh_dc = float(row["grid_wh_dc"].iloc[-1])
    mwh_dc = wh_dc / 1_000_000.0
    return mwh_dc  # MWh DC (giorno)

def read_u_hat(meter_csv: str) -> float:
    df = pd.read_csv(meter_csv)
    if "U_hat" not in df.columns:
        raise SystemExit("meter_out.csv deve avere colonna U_hat")
    u = float(df["U_hat"].iloc[-1])
    return u

def compute_stock_value_energy(mwh_dc: float, eur_per_mwh: float) -> float:
    return mwh_dc * eur_per_mwh

def compute_stock_value_assets(assets: list[dict]) -> tuple[float, list[dict]]:
    total = 0.0
    breakdown = []
    for a in assets:
        v = float(a["committed_units_today"]) * float(a["price_eur_per_unit"])
        total += v
        breakdown.append({
            "name": a["name"],
            "units": float(a["committed_units_today"]),
            "price": float(a["price_eur_per_unit"]),
            "value_eur": v
        })
    return total, breakdown

# ------------------ PURCHASE PLANNER ------------------

def plan_purchases(gap_eur: float,
                   assets: list[dict],
                   energy_price_eur_per_mwh: float,
                   allow_buy_energy: bool,
                   preferred_mix: dict|None) -> tuple[float, list[dict]]:
    """
    Restituisce (filled_eur, plan_list) dove plan_list = [{type:'asset'|'energy', name, units, cost_eur}, ...]
    Strategia:
      - se preferred_mix è None: cheapest-first (asset + energia)
      - se preferred_mix è set, rispetta i pesi sul totale gap_eur (per quanto possibile)
    """
    plan = []
    filled = 0.0
    remaining = gap_eur

    # prepararci set acquistabili
    items = []
    # assets
    for a in assets:
        if float(a.get("max_extra_units", 0.0)) > 0:
            unit_price = float(a["price_eur_per_unit"])
            items.append({
                "type": "asset",
                "name": a["name"],
                "unit_price": unit_price,
                "max_units": float(a["max_extra_units"]),
            })
    # energia
    if allow_buy_energy and energy_price_eur_per_mwh > 0:
        items.append({
            "type": "energy",
            "name": "energy_MWh",
            "unit_price": float(energy_price_eur_per_mwh),
            "max_units": float("inf"),
        })

    if not items:
        return 0.0, []

    if preferred_mix:
        # Alloca per quote (per quanto possibile), poi eventuale residuo in cheapest-first
        targets = []
        total_weight = sum(preferred_mix.values())
        for it in items:
            w = preferred_mix.get(it["name"], preferred_mix.get("energy" if it["type"]=="energy" else it["name"], 0.0))
            if w > 0:
                targets.append((it, w / total_weight))
        # Prima allocazione per mix
        for it, frac in targets:
            budget = remaining * frac
            max_cost = it["unit_price"] * it["max_units"]
            spend = min(budget, max_cost)
            if spend <= 0: 
                continue
            units = spend / it["unit_price"]
            plan.append({"type":it["type"], "name":it["name"], "units":units, "cost_eur":spend})
            remaining -= spend
            filled += spend
            it["max_units"] -= units

        if remaining <= 1e-6:
            return filled, plan
        # Residuo: cheapest-first
        items_sorted = sorted(items, key=lambda x: x["unit_price"])
        for it in items_sorted:
            if remaining <= 1e-6:
                break
            if it["max_units"] <= 0:
                continue
            max_cost = it["unit_price"] * it["max_units"]
            spend = min(remaining, max_cost)
            if spend <= 0:
                continue
            units = spend / it["unit_price"]
            plan.append({"type":it["type"], "name":it["name"], "units":units, "cost_eur":spend})
            remaining -= spend
            filled += spend
        return filled, plan
    else:
        # Cheapest-first pura
        items_sorted = sorted(items, key=lambda x: x["unit_price"])
        for it in items_sorted:
            if remaining <= 1e-6:
                break
            max_cost = it["unit_price"] * it["max_units"]
            spend = min(remaining, max_cost)
            if spend <= 0:
                continue
            units = spend / it["unit_price"]
            plan.append({"type":it["type"], "name":it["name"], "units":units, "cost_eur":spend})
            remaining -= spend
            filled += spend
        return filled, plan

# ------------------ MAIN ------------------

def main():
    ap = argparse.ArgumentParser(description="AIpowerCoin EU Coverage Planner")
    ap.add_argument("--grid", default=GRID_CSV)
    ap.add_argument("--meter", default=METER_CSV)
    ap.add_argument("--sbase", type=float, default=S_BASE)
    ap.add_argument("--eur_per_aicp_target", type=float, default=EUR_PER_AICP_TARGET)
    ap.add_argument("--target_cr", type=float, default=TARGET_CR)
    ap.add_argument("--energy_eur_per_mwh", type=float, default=ENERGY_EUR_PER_MWH)
    ap.add_argument("--allow_buy_energy", action="store_true" if ALLOW_BUY_ENERGY else "store_false",
                    default=ALLOW_BUY_ENERGY)
    args = ap.parse_args()

    # 1) Dati del giorno
    mwh_dc = read_grid_today(args.grid)
    u_hat  = read_u_hat(args.meter)
    S      = float(args.sbase) * float(u_hat)

    # 2) StockValue_€
    stock_energy_eur = compute_stock_value_energy(mwh_dc, args.energy_eur_per_mwh)
    stock_assets_eur, breakdown = compute_stock_value_assets(ASSETS)
    stock_total_eur = stock_energy_eur + stock_assets_eur

    # 3) €/AICP implicito
    eur_per_aicp_impl = stock_total_eur / S if S > 0 else float("nan")

    # 4) Coverage ratio vs target
    denom = S * args.eur_per_aicp_target
    cr = stock_total_eur / denom if denom > 0 else float("inf")

    print("\n=== AIpowerCoin EU — Coverage Report (oggi) ===")
    print(f"U_hat: {u_hat:.3f}   S_base: {args.sbase:,.0f}  =>  S (AICP): {S:,.0f}")
    print(f"Energy: MWh_DC={mwh_dc:,.2f}  @ {args.energy_eur_per_mwh:.2f} €/MWh  =>  {stock_energy_eur:,.0f} €")
    print("Assets:")
    for b in breakdown:
        print(f"  - {b['name']}: units={b['units']:,.0f} @ {b['price']:.4f} €/unit => {b['value_eur']:,.0f} €")
    print(f"StockValue_€ totale: {stock_total_eur:,.0f} €")
    print(f"€/AICP (implicito):  {eur_per_aicp_impl:.4f} €/AICP")
    print(f"Target €/AICP:       {args.eur_per_aicp_target:.4f} €/AICP")
    print(f"Coverage Ratio:      CR = {cr:.3f}   (target: {args.target_cr:.3f})")

    if cr >= args.target_cr:
        print("\nOK: coverage sufficiente. Nessun acquisto richiesto oggi.")
        return

    # 5) Gap e piano acquisti
    required_eur = args.target_cr * args.eur_per_aicp_target * S
    gap_eur = required_eur - stock_total_eur
    print(f"\nGAP da colmare: {gap_eur:,.0f} €  (per CR target)")

    filled_eur, plan = plan_purchases(
        gap_eur=gap_eur,
        assets=ASSETS,
        energy_price_eur_per_mwh=args.energy_eur_per_mwh,
        allow_buy_energy=args.allow_buy_energy,
        preferred_mix=PREFERRED_MIX
    )

    if not plan or filled_eur <= 1e-6:
        print("ATTENZIONE: impossibile proporre un piano (limiti max_extra troppo bassi?).")
        return

    print("\nPiano acquisti suggerito (minimo):")
    for p in plan:
        if p["type"] == "energy":
            print(f"  - BUY energy: {p['units']:,.2f} MWh  => {p['cost_eur']:,.0f} €")
        else:
            print(f"  - BUY {p['name']}: {p['units']:,.0f} units => {p['cost_eur']:,.0f} €")
    print(f"Copertura aggiuntiva: {filled_eur:,.0f} €")

    # coverage previsto dopo acquisti
    post_stock = stock_total_eur + filled_eur
    post_cr = post_stock / (S * args.eur_per_aicp_target)
    print(f"\nCR previsto post-acquisto: {post_cr:.3f}")

if __name__ == "__main__":
    main()
