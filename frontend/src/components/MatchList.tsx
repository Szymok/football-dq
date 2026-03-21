import { useState } from 'react';
import { type LinkedMatch } from '../api';
import { CheckCircle2, AlertCircle } from 'lucide-react';
import MatchDetail from './MatchDetail';

interface Props {
  matches: LinkedMatch[];
}

export default function MatchList({ matches }: Props) {
  const [selectedMatch, setSelectedMatch] = useState<LinkedMatch | null>(null);

  const getSourceColor = (sourceName: string) => {
    const norm = sourceName.toLowerCase();
    if (norm.includes('espn')) return 'var(--color-src-espn)';
    if (norm.includes('understat')) return 'var(--color-src-understat)';
    if (norm.includes('matchhistory')) return 'var(--color-src-matchhistory)';
    if (norm.includes('sofascore')) return 'var(--color-src-sofascore)';
    return 'var(--color-border)';
  };

  return (
    <div className="bg-[var(--color-surface)] border border-[var(--color-border)] rounded-xl shadow-sm overflow-hidden animate-in fade-in duration-500">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--color-border)] bg-[var(--color-bg)]">
              <th className="text-left px-5 py-4 font-semibold text-[var(--color-text-muted)] uppercase tracking-wider text-xs">Date</th>
              <th className="text-right px-5 py-4 font-semibold text-[var(--color-text-muted)] uppercase tracking-wider text-xs">Home</th>
              <th className="text-center px-4 py-4 font-semibold text-[var(--color-text-muted)] uppercase tracking-wider text-xs">Score</th>
              <th className="text-left px-5 py-4 font-semibold text-[var(--color-text-muted)] uppercase tracking-wider text-xs">Away</th>
              <th className="text-center px-5 py-4 font-semibold text-[var(--color-text-muted)] uppercase tracking-wider text-xs">Sources</th>
              <th className="text-center px-5 py-4 font-semibold text-[var(--color-text-muted)] uppercase tracking-wider text-xs">Status</th>
              <th className="text-right px-5 py-4 font-semibold text-[var(--color-text-muted)] uppercase tracking-wider text-xs">xG Diff</th>
            </tr>
          </thead>
          <tbody>
            {matches.map((m) => {
              const sources = JSON.parse(m.sources_json || '[]');
              const sourceNames: string[] = sources.map((s: any) => s.source);
              
              return (
                <tr 
                  key={m.id} 
                  onClick={() => setSelectedMatch(m)}
                  className="border-b border-[var(--color-border)] hover:bg-[var(--color-bg)] transition-colors cursor-pointer group"
                >
                  <td className="px-5 py-4 text-[var(--color-text-muted)] font-mono text-xs">{m.date}</td>
                  <td className="px-5 py-4 text-right font-medium text-[var(--color-text-main)] group-hover:text-[var(--color-brand)] transition-colors">{m.home_team_canonical}</td>
                  <td className="px-4 py-4 text-center">
                    <div className="inline-flex bg-[var(--color-bg)] border border-[var(--color-border)] rounded px-3 py-1 font-mono font-bold text-[var(--color-text-main)] shadow-sm">
                      {m.home_goals ?? '-'} : {m.away_goals ?? '-'}
                    </div>
                  </td>
                  <td className="px-5 py-4 font-medium text-[var(--color-text-main)] group-hover:text-[var(--color-brand)] transition-colors">{m.away_team_canonical}</td>
                  <td className="px-5 py-4 text-center">
                    <div className="flex justify-center gap-1.5 flex-wrap">
                      {sourceNames.map(s => (
                        <div key={s} title={s} className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: getSourceColor(s) }} />
                      ))}
                    </div>
                  </td>
                  <td className="px-5 py-4 text-center">
                    {m.score_agreement ? (
                      <CheckCircle2 className="w-4 h-4 text-[var(--color-success)] mx-auto" />
                    ) : (
                      <AlertCircle className="w-4 h-4 text-[var(--color-danger)] mx-auto animate-pulse" />
                    )}
                  </td>
                  <td className="px-5 py-4 text-right font-mono text-xs">
                    {m.xg_discrepancy != null ? (
                      <div className="flex items-center justify-end gap-2">
                        <span className={m.xg_discrepancy > 0.3 ? 'text-[var(--color-warning)] font-bold' : 'text-[var(--color-text-muted)]'}>
                          {m.xg_discrepancy.toFixed(3)}
                        </span>
                        <div className="w-12 h-1.5 bg-[var(--color-bg)] rounded-full overflow-hidden border border-[var(--color-border)]">
                           <div className="h-full bg-[var(--color-warning)]" style={{ width: `${Math.min(100, m.xg_discrepancy * 100)}%`}} />
                        </div>
                      </div>
                    ) : (
                      <span className="text-[var(--color-text-muted)]">—</span>
                    )}
                  </td>
                </tr>
              );
            })}
            
            {matches.length === 0 && (
              <tr>
                <td colSpan={7} className="px-5 py-8 text-center text-[var(--color-text-muted)]">
                  No matches found matching the criteria.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {selectedMatch && (
        <MatchDetail 
          matchId={selectedMatch.id}
          homeTeam={selectedMatch.home_team_canonical}
          awayTeam={selectedMatch.away_team_canonical}
          date={selectedMatch.date}
          onClose={() => setSelectedMatch(null)}
        />
      )}
    </div>
  );
}
