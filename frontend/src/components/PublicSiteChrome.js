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
  <section className="px-4 py-16 sm:px-6 lg:py-20">
    <div className="max-w-6xl mx-auto">
      {(eyebrow || title || description) && (
        <div className="mb-10 max-w-3xl lg:mb-12">
          {eyebrow && (
            <p className="text-xs font-mono uppercase tracking-[0.3em] text-gold/80 mb-4">{eyebrow}</p>
          )}
          {title && <h2 className="text-3xl font-serif font-bold text-zinc-100 mb-4 sm:text-4xl md:text-5xl">{title}</h2>}
          {description && <p className="text-base leading-relaxed text-zinc-400 sm:text-lg">{description}</p>}
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
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-4 py-4 sm:px-6">
          <Link to="/" className="flex min-w-0 items-center gap-3">
            <Scale className="w-8 h-8 text-gold" />
            <div className="min-w-0">
              <span className="block truncate text-xl font-serif font-bold text-zinc-100">Nexodify</span>
              <p className="text-[11px] font-mono uppercase tracking-[0.25em] text-zinc-500">PeriziaScan</p>
            </div>
          </Link>

          <nav className="order-3 flex w-full flex-wrap items-center gap-2 text-sm md:order-2 md:w-auto md:justify-center">
            {navLinks.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  `rounded-full border px-4 py-2 transition-colors ${
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

          <Button asChild className="order-2 bg-gold px-4 text-zinc-950 hover:bg-gold-dim sm:px-5 md:order-3">
            <Link to="/">Torna alla landing</Link>
          </Button>
        </div>
      </header>

      <section className="relative overflow-hidden px-4 pb-16 pt-20 sm:px-6 sm:pt-24 sm:pb-20">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(212,175,55,0.16),transparent_35%),linear-gradient(180deg,rgba(24,24,27,0.15),rgba(9,9,11,0))]" />
        <div className="relative max-w-6xl mx-auto">
          <div className="max-w-4xl">
            <p className="text-xs font-mono uppercase tracking-[0.35em] text-gold/80 mb-5">{eyebrow}</p>
            <h1 className="mb-6 text-4xl font-serif font-bold tracking-tight text-zinc-100 sm:text-5xl md:text-6xl">{title}</h1>
            <p className="text-base leading-relaxed text-zinc-400 sm:text-lg md:text-xl">{description}</p>
          </div>
          {actions ? <div className="mt-8 sm:mt-10">{actions}</div> : null}
        </div>
      </section>

      {children}

      <footer className="border-t border-zinc-800 px-4 py-12 sm:px-6">
        <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-6 md:flex-row">
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
