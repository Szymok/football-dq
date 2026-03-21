import { useEffect, useState } from 'react'
import { fetchScorecard, fetchLinkedMatches, fetchDqSummary, type LinkedMatch, type DqSummary } from './api'
import { Activity, Database, CheckSquare, Settings2, ShieldAlert, Cpu, BarChart3, AlertCircle, CheckCircle2, Link2 } from 'lucide-react'
import MatchList from './components/MatchList'

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
  const colorClass = isPass ? 'text-[var(--color-success)]' : isWarn ? 'text-[var(--color-warning)]' : 'text-[var(--color-danger)]'
  
  const radius = 60
  const circumference = 2 * Math.PI * radius
  const strokeDashoffset = circumference - (score / 100) * circumference

  return (
    <div className="relative flex flex-col items-center justify-center p-6 bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl shadow-sm h-full">
      <h3 className="text-sm font-semibold text-[var(--color-text-muted)] uppercase tracking-wider mb-6">Overall Data Quality</h3>
      <div className="relative h-40 w-40">
        <svg className="h-full w-full rotate-[-90deg] transform" viewBox="0 0 140 140">
          <circle cx="70" cy="70" r={radius} fill="none" stroke="var(--color-border)" strokeWidth="12" />
          <circle
            cx="70"
            cy="70"
            r={radius}
            fill="none"
            className={`transition-all duration-1000 ease-out flex ${colorClass}`}
            stroke="currentColor"
            strokeWidth="12"
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className={`text-4xl font-bold tracking-tight text-[var(--color-text-main)]`}>
            {score.toFixed(1)}<span className="text-2xl text-[var(--color-text-muted)] ml-0.5">%</span>
          </span>
        </div>
      </div>
    </div>
  )
}

const MetricCard = ({ label, value, icon: Icon, unit = '' }: any) => (
  <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-6 shadow-sm flex flex-col justify-between h-full">
    <div className="flex justify-between items-start mb-4">
      <div className="text-sm font-medium text-[var(--color-text-muted)]">
        {label}
      </div>
      <div className="p-2 bg-[var(--color-bg)] rounded-lg">
        <Icon className="w-5 h-5 text-[var(--color-brand)]" />
      </div>
    </div>
    <div className="flex items-baseline gap-2 mt-auto">
      <span className="text-4xl font-bold tracking-tight text-[var(--color-text-main)]">
        {value}
      </span>
      {unit && <span className="text-sm font-medium text-[var(--color-text-muted)]">{unit}</span>}
    </div>
  </div>
)

const VendorLeaderboard = ({ vendorScores = {} }: { vendorScores: Record<string, number> }) => {
  const entries = Object.entries(vendorScores).sort((a, b) => b[1] - a[1]);

  if (entries.length === 0) {
    return (
      <div className="mt-8 bg-red-50 border border-red-200 rounded-xl p-6">
        <div className="text-sm font-medium text-red-600 flex items-center gap-2">
          <AlertCircle className="w-5 h-5" /> No vendor data available
        </div>
      </div>
    )
  }

  return (
    <div className="mt-6 bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl shadow-sm overflow-hidden">
      <div className="border-b border-[var(--color-border)] px-6 py-5 flex justify-between items-center bg-[var(--color-bg)]/50">
        <h3 className="text-base font-semibold text-[var(--color-text-main)] flex items-center gap-2">
          <BarChart3 className="w-5 h-5 text-[var(--color-text-muted)]" />
          Provider Accuracy Comparison
        </h3>
        <span className="text-xs font-medium text-[var(--color-text-muted)] bg-[var(--color-surface)] px-2.5 py-1 rounded border border-[var(--color-border)]">
          Baseline: fbref
        </span>
      </div>

      <div className="divide-y divide-[var(--color-border)]">
        {entries.map(([vendor, score], idx) => {
          const isPass = score >= 80;
          const barColor = isPass ? 'var(--color-success)' : (score >= 50 ? 'var(--color-warning)' : 'var(--color-danger)');
          
          return (
            <div key={vendor} className="flex items-center px-6 py-5 hover:bg-[var(--color-bg)]/50 transition-colors">
              <div className="w-8 font-mono text-sm text-[var(--color-text-muted)]">
                {idx + 1}.
              </div>
              <div className="w-48 font-medium text-[var(--color-text-main)] capitalize">
                {vendor}
              </div>
              <div className="flex-1 px-4 relative h-8 flex items-center">
                <div className="w-full h-2.5 bg-[var(--color-border)] rounded-full overflow-hidden">
                  <div 
                    className="h-full transition-all duration-1000 ease-out" 
                    style={{ width: `${score}%`, backgroundColor: barColor }}
                  />
                </div>
              </div>
              <div className="w-20 text-right font-mono text-sm font-medium text-[var(--color-text-main)]">
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
  const [activeTab, setActiveTab] = useState<'scorecard' | 'operations' | 'validation' | 'reconciliation'>('reconciliation')
  const [linkedMatches, setLinkedMatches] = useState<LinkedMatch[]>([])
  const [dqSummary, setDqSummary] = useState<DqSummary | null>(null)
  const [reconLoading, setReconLoading] = useState(false)
  const [teamFilter, setTeamFilter] = useState('')

  useEffect(() => {
    fetchScorecard().then((data) => {
      setRun(data)
      setLoading(false)
    })
  }, [])

  useEffect(() => {
    if (activeTab === 'reconciliation' && linkedMatches.length === 0) {
      setReconLoading(true)
      Promise.all([fetchLinkedMatches(), fetchDqSummary()]).then(([matches, summary]) => {
        setLinkedMatches(matches)
        setDqSummary(summary)
        setReconLoading(false)
      })
    }
  }, [activeTab])

  const renderContent = () => {
    if (activeTab === 'reconciliation') {
      const filteredMatches = teamFilter 
        ? linkedMatches.filter(m => 
            m.home_team_canonical.toLowerCase().includes(teamFilter.toLowerCase()) ||
            m.away_team_canonical.toLowerCase().includes(teamFilter.toLowerCase())
          )
        : linkedMatches;

      return (
        <div className="space-y-6 animate-in fade-in duration-500">
          {/* Summary Cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-5 shadow-sm">
              <div className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wider mb-2">Total Matches</div>
              <div className="text-3xl font-bold text-[var(--color-text-main)]">{dqSummary?.total_matches ?? '—'}</div>
            </div>
            <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-5 shadow-sm">
              <div className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wider mb-2">Multi-Source</div>
              <div className="text-3xl font-bold text-blue-500">{dqSummary?.multi_source_matches ?? '—'}</div>
            </div>
            <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-5 shadow-sm">
              <div className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wider mb-2">Score Agreement</div>
              <div className="text-3xl font-bold text-emerald-500">{dqSummary?.score_agreement_pct?.toFixed(1) ?? '—'}%</div>
            </div>
            <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-5 shadow-sm">
              <div className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wider mb-2">Avg xG Diff</div>
              <div className="text-3xl font-bold text-amber-500">{dqSummary?.avg_xg_discrepancy?.toFixed(3) ?? 'N/A'}</div>
            </div>
          </div>

          {/* Filter */}
          <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-4 shadow-sm flex items-center gap-4">
            <input 
              type="text" 
              placeholder="Filter by team name..." 
              value={teamFilter}
              onChange={e => setTeamFilter(e.target.value)}
              className="flex-1 bg-[var(--color-bg)] border border-[var(--color-border)] rounded-lg px-4 py-2.5 text-sm text-[var(--color-text-main)] placeholder:text-[var(--color-text-muted)] focus:outline-none focus:ring-2 focus:ring-[var(--color-brand)] focus:border-transparent"
            />
            <span className="text-sm text-[var(--color-text-muted)]">{filteredMatches.length} matches</span>
          </div>

          {/* Match Table */}
          {reconLoading ? (
            <div className="flex flex-col items-center justify-center p-16 text-[var(--color-text-muted)] bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl shadow-sm">
              <div className="w-10 h-10 border-[3px] border-[var(--color-border)] border-t-[var(--color-brand)] animate-spin rounded-full mb-6"></div>
              <span className="text-base font-medium">Loading linked matches...</span>
            </div>
          ) : (
            <MatchList matches={filteredMatches} />
          )}
        </div>
      );
    }

    if (activeTab === 'operations') {
      return (
        <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-8 shadow-sm">
          <h2 className="text-2xl font-bold tracking-tight mb-3 text-[var(--color-text-main)] flex items-center gap-3">
             <Cpu className="text-[var(--color-brand)] w-6 h-6" />
             Pipeline Operations
          </h2>
          <p className="text-[var(--color-text-muted)] text-base max-w-2xl mb-8">
            Manage data source pipelines, trigger manual reconciliation merges, and monitor ingestion queues.
          </p>
          <div className="bg-[#0f172a] border border-[var(--color-border)] rounded-lg p-6 font-mono text-sm text-gray-300">
            $ systemctl status data-pipeline<br />
            <span className="text-[var(--color-success)]">&gt; Active and running...</span>
          </div>
        </div>
      );
    }

    if (activeTab === 'validation') {
      return (
        <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-8 shadow-sm">
          <h2 className="text-2xl font-bold tracking-tight mb-3 text-[var(--color-text-main)] flex items-center gap-3">
             <CheckSquare className="text-[var(--color-brand)] w-6 h-6" />
             Validation Rules
          </h2>
          <p className="text-[var(--color-text-muted)] text-base max-w-2xl mb-8">
            Detailed breakdown of data quality checks, constraint violations, and statistical anomalies.
          </p>
          <div className="bg-emerald-50 border border-emerald-100 rounded-lg p-6 flex items-center gap-3 text-emerald-700">
             <CheckCircle2 className="w-6 h-6" />
             <span className="font-medium">All validation rules passing successfully.</span>
          </div>
        </div>
      );
    }

    return (
      <div className="space-y-6 animate-in fade-in duration-500">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 auto-rows-fr">
          <div className="lg:col-span-1 h-full">
            <ScoreRing score={run?.overall_score ?? 0} />
          </div>

          <div className="lg:col-span-2 grid grid-cols-2 gap-6 h-full">
            <MetricCard 
              label="Checks Executed" 
              value={run?.total_checks ?? 0} 
              icon={Settings2} 
            />
            <MetricCard 
              label="Anomaly Count" 
              value={(run?.total_checks ?? 0) - (run?.passed_checks ?? 0)} 
              icon={ShieldAlert}
              unit="issues"
            />
          </div>
        </div>

        <VendorLeaderboard vendorScores={run?.vendor_scores || {}} />
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-[var(--color-bg)] flex text-[var(--color-text-main)]">
      
      {/* Sidebar */}
      <aside className="fixed w-20 h-full bg-[var(--color-surface)] border-r border-[var(--color-border)] flex flex-col items-center py-6 z-50 shadow-sm">
        <div className="h-10 w-10 bg-[var(--color-brand)] rounded-xl flex items-center justify-center mb-12 shadow-md shadow-blue-500/20">
          <Database className="w-5 h-5 text-white" />
        </div>
        
        <nav className="flex flex-col gap-4 w-full px-3">
          <button 
            onClick={() => setActiveTab('scorecard')}
            title="Scorecard"
            className={`w-full aspect-square flex items-center justify-center rounded-xl transition-all ${activeTab === 'scorecard' ? 'bg-[var(--color-bg)] text-[var(--color-brand)] shadow-sm border border-[var(--color-border)]' : 'text-[var(--color-text-muted)] hover:bg-[var(--color-bg)] hover:text-[var(--color-text-main)]'}`}
          >
             <Activity className="w-5 h-5" />
          </button>
          <button 
            onClick={() => setActiveTab('operations')}
            title="Pipeline Operations"
            className={`w-full aspect-square flex items-center justify-center rounded-xl transition-all ${activeTab === 'operations' ? 'bg-[var(--color-bg)] text-[var(--color-brand)] shadow-sm border border-[var(--color-border)]' : 'text-[var(--color-text-muted)] hover:bg-[var(--color-bg)] hover:text-[var(--color-text-main)]'}`}
          >
             <Cpu className="w-5 h-5" />
          </button>
          <button 
            onClick={() => setActiveTab('validation')}
            title="Validation Rules"
            className={`w-full aspect-square flex items-center justify-center rounded-xl transition-all ${activeTab === 'validation' ? 'bg-[var(--color-bg)] text-[var(--color-brand)] shadow-sm border border-[var(--color-border)]' : 'text-[var(--color-text-muted)] hover:bg-[var(--color-bg)] hover:text-[var(--color-text-main)]'}`}
          >
             <CheckSquare className="w-5 h-5" />
          </button>
          <button 
            onClick={() => setActiveTab('reconciliation')}
            title="Reconciliation"
            className={`w-full aspect-square flex items-center justify-center rounded-xl transition-all ${activeTab === 'reconciliation' ? 'bg-[var(--color-bg)] text-[var(--color-brand)] shadow-sm border border-[var(--color-border)]' : 'text-[var(--color-text-muted)] hover:bg-[var(--color-bg)] hover:text-[var(--color-text-main)]'}`}
          >
             <Link2 className="w-5 h-5" />
          </button>
        </nav>
      </aside>

      {/* Main Content */}
      <main className="ml-20 flex-1 p-8 lg:p-12 max-w-6xl mx-auto w-full">
        <header className="mb-10 flex flex-col lg:flex-row lg:justify-between lg:items-end gap-6 border-b border-[var(--color-border)] pb-6">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <span className="flex items-center gap-2 bg-emerald-100/80 text-emerald-700 font-semibold text-xs px-2.5 py-1 rounded-full border border-emerald-200">
                 <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                 Live System
              </span>
              <span className="text-[var(--color-text-muted)] text-sm font-medium">Football Data HQ</span>
            </div>
            <h1 className="text-3xl font-bold tracking-tight text-[var(--color-text-main)]">
              Data Quality Scorecard
            </h1>
          </div>
          
          <div className="text-right flex flex-col items-start lg:items-end">
             <div className="text-xs font-semibold text-[var(--color-text-muted)] uppercase tracking-wider mb-1.5">Last Sync</div>
             <div className="text-sm font-medium text-[var(--color-text-main)]">
               {run?.run_at ? new Date(run.run_at + "Z").toLocaleString() : 'Loading...'}
             </div>
          </div>
        </header>

        {loading ? (
          <div className="flex flex-col items-center justify-center p-24 text-[var(--color-text-muted)] bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl shadow-sm">
            <div className="w-10 h-10 border-[3px] border-[var(--color-border)] border-t-[var(--color-brand)] animate-spin rounded-full mb-6"></div>
            <span className="text-base font-medium">Aggregating Data Quality Metrics...</span>
          </div>
        ) : (
          renderContent()
        )}
      </main>
    </div>
  )
}
