import React from 'react';
import { Link, NavLink } from 'react-router-dom';
import { Scale } from 'lucide-react';
import { Button } from './ui/button';

const footerLinks = [
  { label: 'Pacchetti', to: '/pacchetti' },
  { label: 'Supporto', to: '/supporto' },
  { label: 'Termini', to: '/termini' },
  { label: 'Privacy', to: '/privacy' }
];

const navLinks = [
  { label: 'Pacchetti', to: '/pacchetti' },
  { label: 'Supporto', to: '/supporto' }
];

export const PublicSection = ({ eyebrow, title, description, children }) => (
  <section className="py-20 px-6">
    <div className="max-w-6xl mx-auto">
      {(eyebrow || title || description) && (
        <div className="max-w-3xl mb-12">
          {eyebrow && (
            <p className="text-xs font-mono uppercase tracking-[0.3em] text-gold/80 mb-4">{eyebrow}</p>
          )}
          {title && <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-4">{title}</h2>}
          {description && <p className="text-zinc-400 text-lg leading-relaxed">{description}</p>}
        </div>
      )}
      {children}
    </div>
  </section>
);

const PublicSiteChrome = ({ title, description, eyebrow, children, actions }) => {
  return (
    <div className="min-h-screen bg-[#09090b] text-zinc-100">
      <header className="glass sticky top-0 z-50 border-b border-white/5">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between gap-6">
          <Link to="/" className="flex items-center gap-3">
            <Scale className="w-8 h-8 text-gold" />
            <div>
              <span className="text-xl font-serif font-bold text-zinc-100">Nexodify</span>
              <p className="text-[11px] font-mono uppercase tracking-[0.25em] text-zinc-500">PeriziaScan</p>
            </div>
          </Link>

          <nav className="hidden md:flex items-center gap-3 text-sm">
            {navLinks.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  `px-4 py-2 rounded-full border transition-colors ${
                    isActive
                      ? 'border-gold/40 bg-gold/10 text-gold'
                      : 'border-zinc-800 bg-zinc-900/60 text-zinc-400 hover:text-zinc-100 hover:border-zinc-700'
                  }`
                }
              >
                {item.label}
              </NavLink>
            ))}
          </nav>

          <Button asChild className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-5">
            <Link to="/">Torna alla landing</Link>
          </Button>
        </div>
      </header>

      <section className="relative overflow-hidden px-6 pt-24 pb-20">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(212,175,55,0.16),transparent_35%),linear-gradient(180deg,rgba(24,24,27,0.15),rgba(9,9,11,0))]" />
        <div className="relative max-w-6xl mx-auto">
          <div className="max-w-4xl">
            <p className="text-xs font-mono uppercase tracking-[0.35em] text-gold/80 mb-5">{eyebrow}</p>
            <h1 className="text-4xl md:text-6xl font-serif font-bold tracking-tight text-zinc-100 mb-6">{title}</h1>
            <p className="text-lg md:text-xl text-zinc-400 leading-relaxed">{description}</p>
          </div>
          {actions ? <div className="mt-10">{actions}</div> : null}
        </div>
      </section>

      {children}

      <footer className="border-t border-zinc-800 py-12 px-6">
        <div className="max-w-7xl mx-auto flex flex-col md:flex-row items-center justify-between gap-6">
          <div className="flex items-center gap-3">
            <Scale className="w-6 h-6 text-gold" />
            <span className="font-serif font-bold text-zinc-100">Nexodify</span>
          </div>
          <div className="flex flex-wrap items-center justify-center gap-3 text-sm">
            {footerLinks.map((item) => (
              <Link
                key={item.to}
                to={item.to}
                className="px-3 py-1 rounded-full border border-zinc-800 bg-zinc-900/50 text-zinc-400 transition-colors hover:text-zinc-100 hover:border-zinc-700"
              >
                {item.label}
              </Link>
            ))}
          </div>
          <div className="text-center md:text-right">
            <p className="text-zinc-500 text-sm">
              © 2025 Nexodify Forensic Engine. Piattaforma di supporto all'analisi documentale.
            </p>
            <p className="text-zinc-600 text-xs mt-1">
              Non costituisce consulenza legale, fiscale o professionale. Consultare sempre un professionista qualificato.
            </p>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default PublicSiteChrome;
