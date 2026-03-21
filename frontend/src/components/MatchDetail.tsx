import { useEffect, useState } from 'react';
import { fetchMatchStats, type MatchSourceStat } from '../api';
import { X } from 'lucide-react';

interface Props {
  matchId: number;
  homeTeam: string;
  awayTeam: string;
  date: string;
  onClose: () => void;
}

const parseStat = (val: number | null | undefined): string | number => {
  return val == null ? '-' : val;
};

// Returns a source's CSS variable mapping
const getSourceColor = (sourceName: string) => {
  const norm = sourceName.toLowerCase();
  if (norm.includes('espn')) return 'var(--color-src-espn)';
  if (norm.includes('understat')) return 'var(--color-src-understat)';
  if (norm.includes('matchhistory')) return 'var(--color-src-matchhistory)';
  if (norm.includes('sofascore')) return 'var(--color-src-sofascore)';
  return 'var(--color-border)';
};

export default function MatchDetail({ matchId, homeTeam, awayTeam, date, onClose }: Props) {
  const [stats, setStats] = useState<MatchSourceStat[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchMatchStats(matchId).then(data => {
      setStats(data);
      setLoading(false);
    });
  }, [matchId]);

  if (loading) {
    return (
      <div className="fixed inset-0 z-50 bg-black/60 flex items-center justify-center p-4">
        <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl p-8 shadow-2xl animate-pulse">
           Loading match details...
        </div>
      </div>
    );
  }

  // Stat definitions
  const metrics = [
    { key: "goals", label: "Goals (FT)", type: "int" },
    { key: "ht_goals", label: "Goals (HT)", type: "int" },
    { key: "xg", label: "xG", type: "float" },
    { key: "npxg", label: "npxG", type: "float" },
    { key: "shots", label: "Shots", type: "int" },
    { key: "shots_target", label: "Shots on Target", type: "int" },
    { key: "corners", label: "Corners", type: "int" },
    { key: "fouls", label: "Fouls", type: "int" },
    { key: "yellow", label: "Yellow Cards", type: "int" },
    { key: "red", label: "Red Cards", type: "int" },
    { key: "ppda", label: "PPDA", type: "float" },
    { key: "deep", label: "Deep Completions", type: "int" },
  ];

  return (
    <div className="fixed inset-0 z-50 bg-black/80 flex justify-center p-4 lg:p-12 overflow-y-auto w-full h-full animate-in fade-in duration-200">
      <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-2xl shadow-2xl w-full max-w-5xl h-fit max-h-full">
        {/* Header */}
        <div className="sticky top-0 bg-[var(--color-bg)]/90 backdrop-blur-md rounded-t-2xl z-10 border-b border-[var(--color-border)] px-6 py-5 flex justify-between items-center">
          <div>
            <div className="text-sm font-mono text-[var(--color-brand)] mb-1">{date}</div>
            <h2 className="text-2xl font-bold font-sans text-[var(--color-text-main)]">
              {homeTeam} <span className="text-[var(--color-text-muted)] font-normal mx-2">vs</span> {awayTeam}
            </h2>
          </div>
          <button 
            onClick={onClose}
            className="p-2 hover:bg-[var(--color-surface)] rounded-lg transition-colors border border-transparent hover:border-[var(--color-border)] text-[var(--color-text-muted)] hover:text-white"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6">
          <div className="mb-6 flex gap-2 flex-wrap">
             {stats.map(s => (
               <div key={s.id} className="flex items-center gap-2 px-3 py-1.5 rounded bg-[var(--color-bg)] border border-[var(--color-border)] font-mono text-sm shadow-sm pointer-events-none">
                 <div className="w-2.5 h-2.5 rounded-full" style={{backgroundColor: getSourceColor(s.source)}} />
                 <span className="uppercase text-[var(--color-text-main)] font-semibold">{s.source}</span>
               </div>
             ))}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            {metrics.map(metric => {
              // Extract values for home/away per source
              const sourceData = stats.map(s => {
                const hKey = `home_${metric.key}` as keyof MatchSourceStat;
                const aKey = `away_${metric.key}` as keyof MatchSourceStat;
                
                // Some special mappings due to standard naming
                const actualHKey = metric.key === "goals" ? "home_goals" : metric.key === "ht_goals" ? "ht_home_goals" : hKey;
                const actualAKey = metric.key === "goals" ? "away_goals" : metric.key === "ht_goals" ? "ht_away_goals" : aKey;

                return {
                  id: s.id,
                  source: s.source,
                  home: s[actualHKey as keyof MatchSourceStat] as number | null,
                  away: s[actualAKey as keyof MatchSourceStat] as number | null,
                  color: getSourceColor(s.source),
                };
              });

              // Check if any source has data
              if (!sourceData.some(d => d.home != null || d.away != null)) return null;

              return (
                <div key={metric.key} className="bg-[var(--color-bg)] rounded-xl border border-[var(--color-border)] p-5">
                  <div className="text-sm font-semibold uppercase tracking-wider text-[var(--color-text-muted)] mb-4 pb-2 border-b border-[var(--color-border)]">
                    {metric.label}
                  </div>
                  
                  <div className="space-y-3">
                    {sourceData.map(d => {
                      if (d.home == null && d.away == null) return null;
                      
                      const fmt = (x: number | null) => {
                        if (x == null) return '-';
                        return metric.type === "float" ? x.toFixed(2) : x;
                      }

                      return (
                        <div key={d.id} className="flex items-center text-sm font-mono">
                          <div className="w-20 text-[var(--color-text-muted)] uppercase text-xs font-semibold" style={{color: d.color}}>{d.source}</div>
                          <div className="flex-1 text-right text-[var(--color-text-main)] font-bold">{fmt(d.home)}</div>
                          <div className="w-12 text-center text-[var(--color-text-muted)]">:</div>
                          <div className="flex-1 text-left text-[var(--color-text-main)] font-bold">{fmt(d.away)}</div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
