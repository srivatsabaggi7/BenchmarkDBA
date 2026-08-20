import { useEffect, useMemo, useState } from 'react'
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

const databaseColors = ['#9d7bff', '#57d9c4', '#e9a65d', '#f57caa', '#77a7ff', '#c8d66a']
const chartGrid = '#303649'
const tooltipStyle = { background: '#191d29', border: '1px solid #353c52', borderRadius: 10, color: '#e8eaf0' }
const statusLabel = (status) => ({ ok: 'Success', success: 'Success', partial: 'Partial', skipped: 'DNF', dnf: 'DNF' }[status] || 'Partial')
const fmt = (number) => Number.isFinite(Number(number)) ? Number(number).toFixed(1) : '—'

function Panel({ title, subtitle, children }) {
  return <section className="panel"><h3>{title}</h3><p>{subtitle}</p>{children}</section>
}

function Overlay({ title, onClose, children }) {
  return <div className="overlay" onMouseDown={onClose}>
    <section className="modal" onMouseDown={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label={title}>
      <header className="modal-header"><div><b>BENCHMARK ARCHIVE</b><h2>{title}</h2></div><button className="close" onClick={onClose} aria-label="Close">×</button></header>
      {children}
    </section>
  </div>
}

function Bars({ data, colorByName = true }) {
  return <ResponsiveContainer width="100%" height={320}><BarChart data={data}>
    <CartesianGrid stroke={chartGrid} strokeDasharray="3 3" /><XAxis dataKey="name" stroke="#8991a7" /><YAxis stroke="#8991a7" />
    <Tooltip contentStyle={tooltipStyle} cursor={{ fill: '#ffffff09' }} />
    <Bar dataKey="value" radius={[5, 5, 0, 0]}>{data.map((entry, index) => <Cell key={entry.name} fill={colorByName ? entry.color : '#9d7bff'} />)}</Bar>
  </BarChart></ResponsiveContainer>
}

function Charts({ databases }) {
  const completed = databases.filter((db) => db.status !== 'DNF')
  const metricData = (key) => completed.map((db) => ({ name: db.name, value: db[key], color: db.color }))
  const concurrent = [1, 10, 40].map((clients) => Object.assign(
    { clients: `${clients} clients` },
    ...completed.map((db) => ({ [db.name]: db.concurrency.find((entry) => entry.clients === clients)?.qps || 0 })),
  ))

  return <div className="charts">
    <Panel title="Ingestion throughput" subtitle="Nodes per second · higher is better"><Bars data={metricData('ingest')} /></Panel>
    <Panel title="Point lookup latency" subtitle="Warm p50 milliseconds · lower is better"><Bars data={metricData('point')} /></Panel>
    <Panel title="Cold lookup latency" subtitle="First request milliseconds · lower is better"><Bars data={metricData('cold')} /></Panel>
    <Panel title="Concurrent workload" subtitle="Mixed read/write queries per second"><ResponsiveContainer width="100%" height={320}><LineChart data={concurrent}>
      <CartesianGrid stroke={chartGrid} strokeDasharray="3 3" /><XAxis dataKey="clients" stroke="#8991a7" /><YAxis stroke="#8991a7" /><Tooltip contentStyle={tooltipStyle} /><Legend />
      {completed.map((db) => <Line key={db.name} type="monotone" dataKey={db.name} stroke={db.color} strokeWidth={2.5} dot={false} />)}
    </LineChart></ResponsiveContainer></Panel>
    <Panel title="Observed errors" subtitle="Errors recorded during concurrent workload"><Bars data={metricData('errors')} /></Panel>
    <Panel title="Benchmark coverage" subtitle="Platforms completing the test suite"><ResponsiveContainer width="100%" height={320}><PieChart>
      <Pie data={[{ name: 'Completed', value: completed.length }, { name: 'DNF', value: databases.length - completed.length }]} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={105} label>
        <Cell fill="#57d9c4" /><Cell fill="#f57caa" />
      </Pie><Tooltip contentStyle={tooltipStyle} /><Legend />
    </PieChart></ResponsiveContainer></Panel>
  </div>
}

export default function App() {
  const [data, setData] = useState()
  const [page, setPage] = useState('home')
  const [busy, setBusy] = useState(false)
  const [logsOpen, setLogsOpen] = useState(false)
  const [devLogOpen, setDevLogOpen] = useState(false)
  const [filter, setFilter] = useState('All')
  const [expanded, setExpanded] = useState()

  useEffect(() => { fetch('/results.json').then((response) => response.json()).then(setData) }, [])
  const databases = useMemo(() => (data?.databases || data?.platforms || []).map((platform, index) => {
    const concurrency = platform.concurrency || []
    return {
      name: platform.name || platform.platform,
      status: statusLabel(platform.status),
      color: databaseColors[index % databaseColors.length],
      specs: platform.specs || platform.metadata || {},
      description: platform.description || `Benchmark result for ${platform.name || platform.platform}.`,
      caveats: platform.caveats || [],
      ingest: platform.metrics?.ingest || platform.ingest?.nodes_per_sec || 0,
      point: platform.metrics?.point_latency || platform.latencies?.point_lookup?.p50_ms || 0,
      cold: platform.latencies?.point_lookup?.cold_latency_ms || 0,
      errors: concurrency.reduce((total, item) => total + (item.errors || 0), 0),
      concurrency,
    }
  }), [data])
  const rawLog = data?.raw_run_log || []
  const devLog = data?.dev_log || []
  if (!data) return <main className="loading">Reading local benchmark results…</main>

  const begin = () => { setBusy(true); setTimeout(() => { setPage('results'); setBusy(false) }, 1000) }
  return <main>
    {page === 'home' ? <>
      <header className="hero"><b>STATIC BENCHMARK REPORT</b><h1>Welcome to Benchmark Cloud DB Analytics Suite</h1><p>Six graph databases, one repeatable workload, and a transparent record of every result.</p><button className="primary" onClick={begin}>{busy ? 'Compiling local results…' : 'Begin Comparison'} →</button><small>Reads the local results file — no live database connection.</small></header>
      <section className="db-grid">{databases.map((db) => <article className="card" key={db.name} onClick={() => setExpanded(expanded === db.name ? null : db.name)}>
        <div><h2><i style={{ background: db.color }} />{db.name}</h2><span className={`badge ${db.status.toLowerCase()}`}>{db.status}</span></div><p>Graph database benchmark</p>
        {expanded === db.name && <aside><p>{db.description}</p>{Object.entries(db.specs).slice(0, 4).map(([key, value]) => <small key={key}>{key.replaceAll('_', ' ')}: <strong>{String(value)}</strong></small>)}</aside>}<small>{expanded === db.name ? 'Click to collapse' : 'Click for benchmark details'}</small>
      </article>)}</section>
    </> : <>
      <header className="results"><button className="back" onClick={() => setPage('home')}>← Back to Databases</button><b>BENCHMARK RUN COMPLETE</b><h1>Comparison Done!</h1><p>{data.dataset?.nodes?.toLocaleString()} nodes · {data.dataset?.relationships?.toLocaleString()} relationships · figures from local results.json</p></header>
      <Charts databases={databases} /><footer><button className="secondary" onClick={() => setLogsOpen(true)}>View Logs</button><button className="primary" onClick={() => setDevLogOpen(true)}>View Development Logs & Decisions</button></footer>
    </>}
    {logsOpen && <Overlay title="Raw run log" onClose={() => setLogsOpen(false)}><label className="filter">Filter by database <select value={filter} onChange={(event) => setFilter(event.target.value)}><option>All</option>{databases.map((db) => <option key={db.name}>{db.name}</option>)}</select></label><div className="table"><table><thead><tr><th>Timestamp</th><th>Database</th><th>Event</th><th>Status</th></tr></thead><tbody>{rawLog.filter((entry) => filter === 'All' || entry.db === filter).map((entry, index) => <tr key={index}><td>{entry.timestamp}</td><td>{entry.db}</td><td>{entry.event}</td><td>{entry.status}</td></tr>)}</tbody></table>{rawLog.length === 0 && <p className="empty">No raw run entries were recorded.</p>}</div></Overlay>}
    {devLogOpen && <Overlay title="Development logs & decisions" onClose={() => setDevLogOpen(false)}><div className="journal">{devLog.map((entry, index) => <article key={index}><b>{String(index + 1).padStart(2, '0')}</b><div><h3>{entry.title}</h3><p>{entry.body}</p></div></article>)}{devLog.length === 0 && <p className="empty">No development decisions were recorded for this run.</p>}</div></Overlay>}
  </main>
}
