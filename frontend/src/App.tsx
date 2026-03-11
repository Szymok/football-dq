import { useEffect, useState } from 'react'
import { fetchScorecard } from './api'
import { Activity, Database, CheckSquare, Settings2, ShieldAlert, Cpu, TerminalSquare } from 'lucide-react'

// Types
type DQRun = {
  id: number
  run_at: string
  total_checks: number
  passed_checks: number
  overall_score: number
  dimension_scores: Record<string, number>
  vendor_scores?: Record<string, number>
}

// Components
const ScoreRing = ({ score }: { score: number }) => {
  const isPass = score >= 80
  const isWarn = score >= 50 && score < 80
  const colorClass = isPass ? 'text-[var(--color-volt)]' : isWarn ? 'text-[var(--color-amber)]' : 'text-[var(--color-red)]'
  
  const radius = 120
  const circumference = 2 * Math.PI * radius
  const strokeDashoffset = circumference - (score / 100) * circumference

  return (
    <div className="relative flex flex-col items-center justify-center p-0">
      <div className="relative h-80 w-80">
        <svg className="h-full w-full rotate-[-90deg] transform" viewBox="0 0 280 280">
          <circle
            cx="140"
            cy="140"
            r={radius}
            fill="none"
            stroke="var(--color-surface)"
            strokeWidth="2"
          />
          <circle
            cx="140"
            cy="140"
            r={radius - 16}
            fill="none"
            stroke="var(--color-sideline)"
            strokeWidth="1"
            strokeDasharray="4 4"
          />
          {/* Progress circle */}
          <defs>
            <filter id="neonGlow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="12" result="blur1" />
              <feGaussianBlur stdDeviation="24" result="blur2" />
              <feMerge>
                <feMergeNode in="blur2" />
                <feMergeNode in="blur1" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>
          <circle
            cx="140"
            cy="140"
            r={radius}
            fill="none"
            className={`transition-all duration-1000 ease-out ${colorClass}`}
            stroke="currentColor"
            strokeWidth="4"
            strokeLinecap="square"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            filter="url(#neonGlow)"
          />
          {/* Inner accent ring */}
          <circle
             cx="140"
             cy="140"
             r={radius - 8}
             fill="none"
             className={`${colorClass}`}
             stroke="currentColor"
             strokeWidth="1"
             strokeDasharray={circumference}
             strokeDashoffset={strokeDashoffset}
             opacity="0.5"
          />
        </svg>
        
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className={`text-[5.5rem] leading-none font-display font-black tracking-tighter ${colorClass} drop-shadow-[0_0_15px_currentColor]`}>
            {score.toFixed(1)}
          </span>
          <span className="text-xl font-mono text-[var(--color-secondary-text)] tracking-tight">PERCENT</span>
        </div>
      </div>
      <div className="mt-8 text-xs font-mono uppercase tracking-[0.2em] text-[var(--color-muted-text)] flex items-center gap-2 border border-[var(--color-sideline)] px-4 py-1.5 bg-[var(--color-surface)]">
        <Activity className="w-3 h-3 text-[var(--color-volt)] animate-pulse" />
        System Vitality
      </div>
    </div>
  )
}

const MetricBlock = ({ label, value, icon: Icon, unit = '' }: any) => (
  <div className="border-t border-r border-[var(--color-sideline)] p-6 bg-[var(--color-pitch)]/50 backdrop-blur-sm group hover:bg-[var(--color-surface)] transition-colors grid grid-rows-[auto_1fr] relative overflow-hidden">
    <div className="absolute top-0 right-0 w-8 h-8 bg-[var(--color-sideline)] opacity-0 group-hover:opacity-100 transition-opacity clip-path-triangle" style={{ clipPath: 'polygon(100% 0, 0 0, 100% 100%)' }} />
    <div className="flex justify-between items-start mb-6">
      <div className="text-xs font-mono uppercase tracking-[0.05em] text-[var(--color-secondary-text)]">
        {label}
      </div>
      <Icon className="w-4 h-4 text-[var(--color-muted-text)] group-hover:text-[var(--color-volt)] transition-colors" />
    </div>
    <div className="flex items-baseline gap-2 mt-auto">
      <span className="text-5xl font-display font-medium tracking-tight text-[var(--color-primary-text)]">
        {value}
      </span>
      {unit && <span className="text-sm font-mono text-[var(--color-muted-text)]">{unit}</span>}
    </div>
  </div>
)

const VendorLeaderboard = ({ vendorScores = {} }: { vendorScores: Record<string, number> }) => {
  const entries = Object.entries(vendorScores).sort((a, b) => b[1] - a[1]);

  if (entries.length === 0) {
    return (
      <div className="mt-8 border border-[var(--color-red)] bg-red-950/20 p-6">
        <div className="text-xs font-mono text-[var(--color-red)] uppercase tracking-widest flex items-center gap-2">
          <ShieldAlert className="w-4 h-4" /> Data Stream Offline
        </div>
      </div>
    )
  }

  return (
    <div className="mt-12 border border-[var(--color-sideline)] bg-[var(--color-tunnel)]">
      <div className="border-b border-[var(--color-sideline)] p-4 flex justify-between items-center bg-[var(--color-surface)]">
        <h3 className="text-sm font-mono tracking-widest uppercase text-[var(--color-primary-text)] flex items-center gap-2">
          <TerminalSquare className="w-4 h-4 text-[var(--color-volt)]" />
          Extraction Precision Matrix
        </h3>
        <span className="text-xs font-mono text-[var(--color-secondary-text)] bg-[var(--color-pitch)] px-2 py-1 border border-[var(--color-sideline)]">
          BASELINE: FBREF
        </span>
      </div>

      <div className="p-0">
        {entries.map(([vendor, score], idx) => {
          const isPass = score >= 80;
          const barColor = isPass ? 'var(--color-volt)' : (score >= 50 ? 'var(--color-amber)' : 'var(--color-red)');
          
          return (
            <div key={vendor} className="group flex items-center border-b border-[var(--color-sideline)] last:border-0 hover:bg-[var(--color-surface)] transition-colors">
              <div className="w-12 h-16 flex items-center justify-center border-r border-[var(--color-sideline)] font-mono text-xs text-[var(--color-muted-text)]">
                {String(idx + 1).padStart(2, '0')}
              </div>
              <div className="w-48 pl-6 font-display font-bold text-lg tracking-wide uppercase">
                {vendor}
              </div>
              <div className="flex-1 px-8 relative h-16 flex items-center">
                {/* Harsh technical progress bar */}
                <div className="w-full h-[2px] bg-[var(--color-sideline)] relative">
                  <div 
                    className="absolute top-0 left-0 h-full transition-all duration-1000 ease-out z-10" 
                    style={{ width: `${score}%`, backgroundColor: barColor, boxShadow: `0 0 8px ${barColor}` }}
                  />
                  {/* Marker lines for data-vis feel */}
                  <div className="absolute top-[-4px] left-[25%] w-[1px] h-[10px] bg-[var(--color-muted-text)]" />
                  <div className="absolute top-[-4px] left-[50%] w-[1px] h-[10px] bg-[var(--color-muted-text)]" />
                  <div className="absolute top-[-4px] left-[75%] w-[1px] h-[10px] bg-[var(--color-muted-text)]" />
                </div>
              </div>
              <div className="w-24 text-right pr-6 font-mono text-xl font-medium" style={{ color: barColor }}>
                {score.toFixed(1)}%
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default function App() {
  const [run, setRun] = useState<DQRun | null>(null)
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'scorecard' | 'operations' | 'validation'>('scorecard')

  useEffect(() => {
    fetchScorecard().then((data) => {
      setRun(data)
      setLoading(false)
    })
  }, [])

  const renderContent = () => {
    if (activeTab === 'operations') {
      return (
        <div className="border border-[var(--color-sideline)] bg-[var(--color-tunnel)] p-8">
          <h2 className="text-2xl font-display font-black tracking-tight mb-4 flex items-center gap-3">
             <Cpu className="text-[var(--color-volt)]" />
             Core Operations
          </h2>
          <p className="text-[var(--color-secondary-text)] font-mono text-sm max-w-2xl mb-8">
            Manage data source pipelines, trigger manual reconciliation merges, and monitor ingestion queues.
          </p>
          <div className="border border-[var(--color-sideline)] bg-[var(--color-pitch)] p-6 font-mono text-sm text-[var(--color-muted-text)]">
            &gt; SYSTEM_READY
            <br />
            &gt; WAITING_FOR_OPERATOR_INPUT...
          </div>
        </div>
      );
    }

    if (activeTab === 'validation') {
      return (
        <div className="border border-[var(--color-sideline)] bg-[var(--color-tunnel)] p-8">
          <h2 className="text-2xl font-display font-black tracking-tight mb-4 flex items-center gap-3">
             <CheckSquare className="text-[var(--color-volt)]" />
             Validation Matrix
          </h2>
          <p className="text-[var(--color-secondary-text)] font-mono text-sm max-w-2xl mb-8">
            Detailed breakdown of data quality checks, constraint violations, and statistical anomalies.
          </p>
          <div className="border border-[var(--color-sideline)] bg-[var(--color-pitch)] p-6 font-mono text-sm text-[var(--color-volt)] flex w-full justify-between items-center opacity-70">
             <span>NO_ACTIVE_VIOLATIONS_DETECTED</span>
             <ShieldAlert className="w-5 h-5 text-[var(--color-volt)] animate-pulse" />
          </div>
        </div>
      );
    }

    // Default to scorecard
    return (
      <div className="space-y-12 animate-in fade-in duration-500">
        {/* Top Grid - Focus on Hero Asset vs KPIs */}
        <div className="grid grid-cols-1 lg:grid-cols-12 border border-[var(--color-sideline)] bg-[var(--color-tunnel)]">
          
          {/* Massive Hero Radial left-aligned */}
          <div className="lg:col-span-5 border-r border-[var(--color-sideline)] p-12 flex items-center justify-center bg-[var(--color-pitch)] relative overflow-hidden">
            {/* Aesthetic background noise / grid */}
            <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjAiIGhlaWdodD0iMjAiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PGNpcmNsZSBjeD0iMiIgY3k9IjIiIHI9IjEiIGZpbGw9IiMzMzMiLz48L3N2Zz4=')] opacity-20 mask-image:linear-gradient(to_bottom,black,transparent)" style={{ WebkitMaskImage: 'radial-gradient(black, transparent)' }} />
            <ScoreRing score={run?.overall_score ?? 0} />
          </div>

          {/* Data Dense KPIs */}
          <div className="lg:col-span-7 grid grid-cols-2 grid-rows-2">
            <MetricBlock 
              label="Checks Executed" 
              value={run?.total_checks ?? 0} 
              icon={Settings2} 
            />
            <MetricBlock 
              label="Anomaly Count" 
              value={(run?.total_checks ?? 0) - (run?.passed_checks ?? 0)} 
              icon={ShieldAlert}
              unit="FLAGS"
            />
            <div className="col-span-2 border-t border-[var(--color-sideline)] p-6 bg-[var(--color-surface)] flex justify-between items-center">
              <div className="flex flex-col gap-1">
                 <span className="text-[0.65rem] font-mono uppercase tracking-widest text-[var(--color-muted-text)]">Last Synchronized</span>
                 <span className="font-mono text-sm text-[var(--color-primary-text)]">
                   {run?.run_at ? new Date(run.run_at + "Z").toISOString().replace('T', ' ').substring(0, 19) : 'UNKNOWN'}
                 </span>
              </div>
              <div className="px-3 py-1.5 border border-[var(--color-volt)] text-[var(--color-volt)] text-[0.65rem] font-mono uppercase tracking-widest">
                Telemetry Active
              </div>
            </div>
          </div>

        </div>

        {/* Vendor Leaderboard Section */}
        <VendorLeaderboard vendorScores={run?.vendor_scores || {}} />
      </div>
    );
  };

  return (
    <div className="min-h-screen flex text-[var(--color-primary-text)] selection:bg-[var(--color-volt)] selection:text-black">
      
      {/* Sidebar - Brutalist Vertical Strip */}
      <aside className="fixed w-20 h-full border-r border-[var(--color-sideline)] bg-[var(--color-pitch)] flex flex-col items-center py-8 z-50">
        <div className="h-12 w-12 border border-[var(--color-volt)] bg-[var(--color-volt)]/10 flex items-center justify-center rotate-45 mb-16 shadow-[0_0_15px_var(--color-volt)]">
          <Database className="w-5 h-5 text-[var(--color-volt)] -rotate-45" />
        </div>
        
        <nav className="flex flex-col gap-8 opacity-50">
          <button 
            onClick={() => setActiveTab('scorecard')}
            className={`w-8 h-8 flex items-center justify-center transition-opacity cursor-pointer ${activeTab === 'scorecard' ? 'opacity-100 border-b-2 border-[var(--color-volt)] text-[var(--color-primary-text)]' : 'hover:opacity-100'}`}
          >
             <Activity className="w-5 h-5" />
          </button>
          <button 
            onClick={() => setActiveTab('operations')}
            className={`w-8 h-8 flex items-center justify-center transition-opacity cursor-pointer ${activeTab === 'operations' ? 'opacity-100 border-b-2 border-[var(--color-volt)] text-[var(--color-primary-text)]' : 'hover:opacity-100'}`}
          >
             <Cpu className="w-5 h-5" />
          </button>
          <button 
            onClick={() => setActiveTab('validation')}
            className={`w-8 h-8 flex items-center justify-center transition-opacity cursor-pointer ${activeTab === 'validation' ? 'opacity-100 border-b-2 border-[var(--color-volt)] text-[var(--color-primary-text)]' : 'hover:opacity-100'}`}
          >
             <CheckSquare className="w-5 h-5" />
          </button>
        </nav>
      </aside>

      {/* Main Container */}
      <main className="ml-20 flex-1 p-8 lg:p-16 max-w-7xl mx-auto">
        <header className="mb-16 border-b border-[var(--color-sideline)] pb-8 flex flex-col lg:flex-row lg:justify-between lg:items-end gap-6">
          <div>
            <h1 className="text-6xl font-display font-black tracking-tighter uppercase leading-[0.85]">
              Data Quality <br />
              <span className="text-transparent bg-clip-text bg-gradient-to-r from-[var(--color-volt)] to-[var(--color-primary-text)]">
                Scorecard
              </span>
            </h1>
            <div className="mt-6 flex items-center gap-4 text-xs font-mono uppercase tracking-widest text-[var(--color-secondary-text)]">
              <span className="flex items-center gap-2 border border-[var(--color-sideline)] px-3 py-1 bg-[var(--color-surface)]">
                 <span className="w-1.5 h-1.5 rounded-full bg-[var(--color-volt)] animate-pulse" />
                 LIVE
              </span>
              <span>FOOTBALL_DATA_HQ</span>
            </div>
          </div>
          
          <div className="text-right flex flex-col items-end">
             <div className="text-[0.65rem] font-mono tracking-[0.2em] text-[var(--color-muted-text)] mb-2 uppercase">Endpoint Stream</div>
             <div className="font-mono text-sm px-4 py-2 border border-[var(--color-sideline)] bg-[var(--color-tunnel)] text-[var(--color-primary-text)]">
               ws://127.0.0.1:8000/stream
             </div>
          </div>
        </header>

        {loading ? (
          <div className="flex gap-4 items-center">
            <div className="w-6 h-6 border-2 border-[var(--color-volt)] border-t-transparent animate-spin rounded-full"></div>
            <span className="font-mono text-sm uppercase tracking-widest text-[var(--color-secondary-text)]">Initializing Data Pipeline...</span>
          </div>
        ) : (
          renderContent()
        )}

      </main>
    </div>
  )
}
