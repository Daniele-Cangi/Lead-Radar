import React, { useEffect, useMemo, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { ReloadIcon } from "@radix-ui/react-icons";
import { Download, Globe, HeartPulse, ListFilter, Play, RefreshCcw, Server, Share2, Sun, Moon, ChevronRight, Cloud } from "lucide-react";

// LeadRadar Pro Dashboard (single-file React component)
// - Polished UI with shadcn/ui, Tailwind and lucide icons
// - Works against your existing API: /health, /v1/jobs, /v1/jobs/scan (POST), /v1/export (POST), /v1/leads?limit=
// - Zero external state mgmt: simple hooks + fetch wrappers
// - Dark mode toggle, compact tables, inline feedback, resilient error handling

// Utility: fetch wrapper with base URL
async function api(baseUrl, path, init) {
  const res = await fetch(`${baseUrl}${path}`, init);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const ct = res.headers.get("content-type") || "";
  return ct.includes("application/json") ? res.json() : res.text();
}

function useDarkMode() {
  const [dark, setDark] = useState(() => {
    if (typeof window === "undefined") return false;
    return document.documentElement.classList.contains("dark");
  });
  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
  }, [dark]);
  return { dark, setDark };
}

export default function LeadRadarDashboard() {
  const { dark, setDark } = useDarkMode();
  const [baseUrl, setBaseUrl] = useState("http://127.0.0.1:5050");

  // Health
  const [health, setHealth] = useState(null);
  const [loadingHealth, setLoadingHealth] = useState(false);
  const loadHealth = async () => {
    setLoadingHealth(true);
  try { setHealth(await api(baseUrl, "/health")); }
  catch (e) { setHealth({ error: String(e) }); }
    finally { setLoadingHealth(false); }
  };

  // Jobs list
  const [jobs, setJobs] = useState([]);
  const [loadingJobs, setLoadingJobs] = useState(false);
  const loadJobs = async () => {
    setLoadingJobs(true);
    try {
      const d = await api(baseUrl, "/v1/jobs");
      setJobs(d.items ?? d ?? []);
  } catch(e){
      setJobs([{ id: "error", error: String(e) }]);
    } finally { setLoadingJobs(false); }
  };

  // Leads preview
  const [leads, setLeads] = useState([]);
  const [leadsLimit, setLeadsLimit] = useState(100);
  const [loadingLeads, setLoadingLeads] = useState(false);
  const loadLeads = async () => {
    setLoadingLeads(true);
    try {
      const d = await api(baseUrl, `/v1/leads?limit=${leadsLimit}`);
      setLeads(d.items ?? d ?? []);
  } catch(e){
      setLeads([{ id: "error", error: String(e) }]);
    } finally { setLoadingLeads(false); }
  };

  // Scan form
  const [countries, setCountries] = useState("EU_EEA_PLUS");
  const [sources, setSources] = useState("ALL");
  const [mps, setMps] = useState(300);
  const [sinceMonths, setSinceMonths] = useState(18);
  const [scanMsg, setScanMsg] = useState("");
  const [scanning, setScanning] = useState(false);

  const startScan = async () => {
    setScanning(true); setScanMsg("Starting…");
    try {
      const payload = {
        countries: countries.split(",").map(s=>s.trim()).filter(Boolean),
        sources: sources.split(",").map(s=>s.trim()).filter(Boolean),
        max_per_source: Number(mps),
        since_months: Number(sinceMonths)
      };
      const d = await api(baseUrl, "/v1/jobs/scan", { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify(payload) });
      setScanMsg(`OK • job_id=${d.job_id ?? "(see /v1/jobs)"}`);
      loadJobs();
  } catch(e) {
      setScanMsg(`ERROR • ${String(e)}`);
    } finally { setScanning(false); }
  };

  // Export form
  const [formats, setFormats] = useState(["csv", "md"]);
  const [exportMsg, setExportMsg] = useState("");
  const [exporting, setExporting] = useState(false);
  const runExport = async () => {
    setExporting(true); setExportMsg("Exporting…");
    try {
      const d = await api(baseUrl, "/v1/export", { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ format: formats })});
      setExportMsg(JSON.stringify(d, null, 2));
  } catch(e){
      setExportMsg(`ERROR • ${String(e)}`);
    } finally { setExporting(false); }
  };

  // Derived KPIs
  const kpi = useMemo(() => {
    const totalLeads = leads?.length || 0;
  const withEmail = leads.filter((l)=>Array.isArray(l.emails_found||l.emails) ? (l.emails_found||l.emails).length>0 : !!l.email).length;
    return {
      totalLeads,
      contactable: withEmail,
      pctContactable: totalLeads ? Math.round(100*withEmail/totalLeads) : 0
    };
  }, [leads]);

  useEffect(() => {
    loadHealth();
    loadJobs();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [baseUrl]);

  return (
    <div className="min-h-screen bg-gradient-to-b from-background to-muted/30 p-4 md:p-8">
      <div className="mx-auto max-w-7xl space-y-6">
        {/* Header */}
        <header className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div className="space-y-1">
            <h1 className="text-3xl font-semibold tracking-tight flex items-center gap-2">
              <Globe className="h-7 w-7"/> LeadRadar <span className="text-muted-foreground text-base">Pro</span>
            </h1>
            <p className="text-muted-foreground">Global tech lead intelligence • FastAPI backend at <span className="font-mono text-sm px-1.5 py-0.5 rounded bg-muted">{baseUrl}</span></p>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2"><Sun className="h-4 w-4"/><Switch checked={dark} onCheckedChange={(v)=>setDark(!!v)} /><Moon className="h-4 w-4"/></div>
          </div>
        </header>

        {/* Controls Row */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2"><Server className="h-5 w-5"/> Backend</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center gap-2">
                <Input value={baseUrl} onChange={(e)=>setBaseUrl(e.target.value)} placeholder="http://127.0.0.1:5050"/>
                <Button variant="secondary" onClick={loadHealth} disabled={loadingHealth}>
                  {loadingHealth ? <ReloadIcon className="mr-2 h-4 w-4 animate-spin"/> : <HeartPulse className="mr-2 h-4 w-4"/>}
                  Health
                </Button>
              </div>
              <div className="text-sm text-muted-foreground font-mono whitespace-pre-wrap max-h-24 overflow-auto rounded bg-muted p-2">
                {health ? JSON.stringify(health, null, 2) : "No data yet"}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2"><ListFilter className="h-5 w-5"/> Quick Scan</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="grid grid-cols-2 gap-2">
                <div className="col-span-2">
                  <Label>Countries (comma)</Label>
                  <Input value={countries} onChange={(e)=>setCountries(e.target.value)} />
                </div>
                <div className="col-span-2">
                  <Label>Sources (comma)</Label>
                  <Input value={sources} onChange={(e)=>setSources(e.target.value)} />
                </div>
                <div>
                  <Label>Max per source</Label>
                  <Input type="number" value={mps} onChange={(e)=>setMps(Number(e.target.value))} />
                </div>
                <div>
                  <Label>Since months</Label>
                  <Input type="number" value={sinceMonths} onChange={(e)=>setSinceMonths(Number(e.target.value))} />
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Button onClick={startScan} disabled={scanning}>
                  {scanning ? <ReloadIcon className="mr-2 h-4 w-4 animate-spin"/> : <Play className="mr-2 h-4 w-4"/>}
                  Start
                </Button>
                <span className="text-sm text-muted-foreground font-mono">{scanMsg}</span>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2"><Share2 className="h-5 w-5"/> Export</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center gap-2">
                <Select onValueChange={(v)=>{
                  // toggle in multiselect fashion for demo simplicity
                  setFormats(prev => prev.includes(v) ? prev.filter(x=>x!==v) : [...prev, v]);
                }}>
                  <SelectTrigger className="w-40"><SelectValue placeholder="Select format" /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="csv">csv</SelectItem>
                    <SelectItem value="jsonl">jsonl</SelectItem>
                    <SelectItem value="md">md</SelectItem>
                  </SelectContent>
                </Select>
                <div className="flex flex-wrap gap-1">
                  {formats.map(f=>(<Badge key={f} variant="secondary" className="uppercase">{f}</Badge>))}
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Button variant="secondary" onClick={runExport} disabled={exporting}>
                  {exporting ? <ReloadIcon className="mr-2 h-4 w-4 animate-spin"/> : <Download className="mr-2 h-4 w-4"/>}
                  Export
                </Button>
                <span className="text-sm text-muted-foreground font-mono truncate max-w-[60%]">{exportMsg}</span>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Jobs & Leads */}
        <Tabs defaultValue="jobs" className="w-full">
          <TabsList>
            <TabsTrigger value="jobs" className="gap-2"><RefreshCcw className="h-4 w-4"/> Jobs</TabsTrigger>
            <TabsTrigger value="leads" className="gap-2"><Cloud className="h-4 w-4"/> Leads</TabsTrigger>
          </TabsList>

          <TabsContent value="jobs" className="mt-4">
            <Card>
              <CardHeader className="flex-row items-center justify-between">
                <CardTitle className="flex items-center gap-2">Recent Jobs</CardTitle>
                <div className="flex items-center gap-2">
                  <Button size="sm" variant="secondary" onClick={loadJobs} disabled={loadingJobs}>
                    {loadingJobs ? <ReloadIcon className="mr-2 h-4 w-4 animate-spin"/> : <RefreshCcw className="mr-2 h-4 w-4"/>}
                    Refresh
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="rounded-md border overflow-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>ID</TableHead>
                        <TableHead>Type</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead>Created</TableHead>
                        <TableHead>Region</TableHead>
                        <TableHead>Sources</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {(jobs?.length? jobs: [{id:"-", type:"-", status:"-"}]).slice(0,50).map((j)=> (
                        <TableRow key={j.id}>
                          <TableCell className="font-mono text-xs">{j.id}</TableCell>
                          <TableCell>{j.type ?? j.kind ?? "scan"}</TableCell>
                          <TableCell>
                            <Badge variant={j.status==="done"?"default": j.status==="running"?"secondary":"outline"}>{j.status ?? "?"}</Badge>
                          </TableCell>
                          <TableCell className="text-muted-foreground text-sm">{j.created_at ?? j.created ?? ""}</TableCell>
                          <TableCell className="text-sm">{(j.params?.countries||[]).join(", ")}</TableCell>
                          <TableCell className="text-sm">{(j.params?.sources||[]).join(", ")}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="leads" className="mt-4">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <Card>
                <CardHeader className="pb-2"><CardTitle>Overview</CardTitle></CardHeader>
                <CardContent className="grid grid-cols-3 gap-3">
                  <div>
                    <div className="text-muted-foreground text-xs">Leads</div>
                    <div className="text-2xl font-semibold">{kpi.totalLeads}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs">Contactable</div>
                    <div className="text-2xl font-semibold">{kpi.contactable}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs">% Contactable</div>
                    <div className="text-2xl font-semibold">{kpi.pctContactable}%</div>
                  </div>
                  <div className="col-span-3 text-xs text-muted-foreground">Preview limits to the last {leadsLimit} items.</div>
                </CardContent>
              </Card>

              <Card className="lg:col-span-2">
                <CardHeader className="flex-row justify-between items-center">
                  <CardTitle className="flex items-center gap-2">Leads Preview</CardTitle>
                  <div className="flex items-center gap-2">
                    <Input className="w-28" type="number" value={leadsLimit} onChange={(e)=>setLeadsLimit(Number(e.target.value))} />
                    <Button size="sm" variant="secondary" onClick={loadLeads} disabled={loadingLeads}>
                      {loadingLeads ? <ReloadIcon className="mr-2 h-4 w-4 animate-spin"/> : <RefreshCcw className="mr-2 h-4 w-4"/>}
                      Load
                    </Button>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="rounded-md border overflow-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Company</TableHead>
                          <TableHead>Name</TableHead>
                          <TableHead>Email</TableHead>
                          <TableHead>Phone</TableHead>
                          <TableHead>Country</TableHead>
                          <TableHead>Score</TableHead>
                          <TableHead>Source</TableHead>
                          <TableHead>URL</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {(leads?.length? leads: [{id:"-", company:"-"}]).map((r, i)=> (
                          <TableRow key={r.id ?? i}>
                            <TableCell>{r.company ?? r.org ?? ""}</TableCell>
                            <TableCell>{r.name ?? r.contact_name ?? ""}</TableCell>
                            <TableCell className="font-mono text-xs">{Array.isArray(r.emails_found) ? r.emails_found?.[0] ?? "" : (r.email ?? "")}</TableCell>
                            <TableCell className="font-mono text-xs">{Array.isArray(r.phones_found) ? r.phones_found?.[0] ?? "" : (r.phone ?? "")}</TableCell>
                            <TableCell>{r.country ?? r.region ?? ""}</TableCell>
                            <TableCell><Badge variant="secondary">{r.score ?? r.priority ?? ""}</Badge></TableCell>
                            <TableCell>{r.source ?? r.channel ?? ""}</TableCell>
                            <TableCell>
                              {r.source_url ? (
                                <a href={r.source_url} target="_blank" className="text-primary underline underline-offset-2">link</a>
                              ) : ""}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>

        <footer className="pt-6 text-center text-xs text-muted-foreground">© {new Date().getFullYear()} LeadRadar • Professional UI</footer>
      </div>
    </div>
  );
}
