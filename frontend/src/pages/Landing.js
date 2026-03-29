import React, { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Button } from '../components/ui/button';
import PreLoginNoticeDialog from '../components/PreLoginNoticeDialog';
import { 
  FileText, 
  Shield, 
  ArrowRight,
  Scale,
  Building2,
  Calculator,
  AlertTriangle,
  Sparkles,
  ScanSearch,
  BadgeCheck,
  BriefcaseBusiness,
  Landmark
} from 'lucide-react';
import PublicPlansGrid from '../components/PublicPlansGrid';

const Landing = () => {
  const { user, login } = useAuth();
  const navigate = useNavigate();
  const [isLoginNoticeOpen, setIsLoginNoticeOpen] = useState(false);

  useEffect(() => {
    if (user) {
      navigate('/dashboard');
    }
  }, [user, navigate]);

  const openLoginNotice = () => {
    setIsLoginNoticeOpen(true);
  };

  const handleConfirmLogin = () => {
    setIsLoginNoticeOpen(false);
    login();
  };

  const features = [
    {
      icon: <FileText className="w-6 h-6" />,
      title_it: "Alert con riferimenti di pagina",
      description_it: "Ogni segnalazione importante e collegata alle pagine della perizia, cosi il controllo resta ancorato al documento"
    },
    {
      icon: <Shield className="w-6 h-6" />,
      title_it: "Non una semplice sintesi AI",
      description_it: "Il report non si limita a riassumere: mette in evidenza criticita, costi da verificare e punti che contano per decidere"
    },
    {
      icon: <Calculator className="w-6 h-6" />,
      title_it: "Più rapido del primo screening manuale",
      description_it: "Riduce il tempo speso a cercare passaggi chiave, senza trasformare la verifica in una black box opaca"
    },
    {
      icon: <AlertTriangle className="w-6 h-6" />,
      title_it: "Criticita legali da verificare",
      description_it: "Blocchi, oneri e punti sensibili emergono in una vista iniziale orientata alla decisione"
    },
    {
      icon: <ScanSearch className="w-6 h-6" />,
      title_it: "Pensato per decisioni d’asta serie",
      description_it: "Nasce per chi deve selezionare opportunita e scartare dossier deboli prima di investire tempo professionale"
    },
    {
      icon: <AlertTriangle className="w-6 h-6" />,
      title_it: "Semaforo di Rischio",
      description_it: "Vista sintetica del livello di rischio per orientare piu rapidamente il pre-screening"
    }
  ];

  return (
    <div className="min-h-screen bg-[#09090b]">
      {/* Header */}
      <header className="glass fixed top-0 w-full z-50 border-b border-white/5">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Scale className="w-8 h-8 text-gold" />
            <span className="text-xl font-serif font-bold text-zinc-100">Nexodify</span>
          </div>
          <Button 
            onClick={openLoginNotice}
            data-testid="header-login-btn"
            className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-6"
          >
            Accedi / Login
          </Button>
        </div>
      </header>

      {/* Hero Section */}
      <section className="relative pt-32 pb-24 px-6 overflow-hidden">
        <div 
          className="absolute inset-0 opacity-20"
          style={{
            backgroundImage: 'url(https://images.pexels.com/photos/13498650/pexels-photo-13498650.jpeg)',
            backgroundSize: 'cover',
            backgroundPosition: 'center'
          }}
        />
        <div className="absolute inset-0 bg-gradient-to-b from-transparent via-[#09090b]/80 to-[#09090b]" />
        
        <div className="relative max-w-7xl mx-auto text-center stagger-children">
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-gold/10 border border-gold/20 mb-8">
            <Sparkles className="w-4 h-4 text-gold" />
            <span className="text-sm font-mono text-gold">Screening premium, alert ancorati alle pagine della perizia</span>
          </div>
          
          <h1 className="text-5xl md:text-7xl font-serif font-bold text-zinc-100 tracking-tight leading-none mb-6">
            Screening della perizia, con prove di pagina.
          </h1>
          
          <p className="text-lg md:text-xl text-zinc-400 max-w-3xl mx-auto mb-12 leading-relaxed">
            PeriziaScan non e una semplice sintesi AI. Trasforma perizie lunghe e opache in uno screening piu rapido della prima revisione manuale, con alert, criticita e costi da verificare collegati direttamente alle pagine del documento.
          </p>

          <div className="mx-auto mb-12 grid max-w-5xl grid-cols-1 gap-4 md:grid-cols-3">
            {[
              'Ogni alert importante e collegato alle pagine della perizia',
              'Piu difendibile di un riassunto generico',
              'Pensato per decidere in fretta su aste serie'
            ].map((item) => (
              <div key={item} className="section-fade rounded-2xl border border-white/10 bg-black/25 px-5 py-4 text-sm text-zinc-200 backdrop-blur-sm">
                {item}
              </div>
            ))}
          </div>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Button 
              onClick={openLoginNotice}
              data-testid="hero-get-started-btn"
              className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-8 py-6 text-lg gold-glow gold-glow-hover"
            >
              Inizia con 12 crediti inclusi <ArrowRight className="w-5 h-5 ml-2" />
            </Button>
            <Button 
              variant="outline"
              data-testid="hero-learn-more-btn"
              className="border-zinc-700 text-zinc-300 hover:bg-zinc-800 px-8 py-6 text-lg"
              onClick={() => document.getElementById('features').scrollIntoView({ behavior: 'smooth' })}
            >
              Vedi cosa controlla
            </Button>
          </div>
        </div>
      </section>

      {/* Fit Strip */}
      <section className="py-8 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="section-fade grid grid-cols-2 gap-3 md:grid-cols-4">
            {[
              { icon: <BriefcaseBusiness className="w-4 h-4" />, label: 'Per investitori' },
              { icon: <BadgeCheck className="w-4 h-4" />, label: 'Per consulenti' },
              { icon: <Landmark className="w-4 h-4" />, label: 'Per studi legali' },
              { icon: <ScanSearch className="w-4 h-4" />, label: 'Per screening rapido delle aste' },
            ].map((item) => (
              <div key={item.label} className="rounded-2xl border border-zinc-800 bg-zinc-900/70 px-4 py-3 text-sm text-zinc-300 flex items-center justify-center gap-2">
                <span className="text-gold">{item.icon}</span>
                <span>{item.label}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Semaforo Preview */}
      <section className="py-16 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-8">
            <p className="text-zinc-500 text-sm md:text-base">
              Una vista immediata per capire dove concentrare le verifiche iniziali.
            </p>
          </div>
          <div className="section-fade grid grid-cols-3 gap-4 max-w-lg mx-auto">
            <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-6 text-center">
              <div className="w-12 h-12 rounded-full bg-red-500 mx-auto mb-3 animate-pulse-slow" />
              <p className="text-red-400 font-mono text-sm font-bold">ALTO RISCHIO</p>
            </div>
            <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-6 text-center">
              <div className="w-12 h-12 rounded-full bg-amber-500 mx-auto mb-3" />
              <p className="text-amber-400 font-mono text-sm font-bold">ATTENZIONE</p>
            </div>
            <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-xl p-6 text-center">
              <div className="w-12 h-12 rounded-full bg-emerald-500 mx-auto mb-3" />
              <p className="text-emerald-400 font-mono text-sm font-bold">BASSO RISCHIO</p>
            </div>
          </div>
        </div>
      </section>

      {/* Why */}
      <section id="features" className="py-24 px-6 bg-zinc-900/30">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-4">
              Perche PeriziaScan
            </h2>
            <p className="text-zinc-400 text-lg">
              Un posizionamento preciso: meno testo generico, piu lettura strutturata utile a una decisione reale
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {features.map((feature, index) => (
              <div 
                key={index}
                className="group bg-zinc-900 border border-zinc-800 hover:border-zinc-600 rounded-xl p-6 transition-all duration-300 card-hover"
              >
                <div className="w-12 h-12 rounded-lg bg-gold/10 flex items-center justify-center text-gold mb-4 group-hover:bg-gold/20 transition-colors">
                  {feature.icon}
                </div>
                <h3 className="text-xl font-semibold text-zinc-100 mb-2">
                  {feature.title_it}
                </h3>
                <p className="text-zinc-400 text-sm leading-relaxed">{feature.description_it}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Comparison */}
      <section className="py-24 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-4">
              Più utile di un riassunto generico
            </h2>
            <p className="text-zinc-400 text-lg">
              La differenza non e “piu testo”. La differenza e come viene costruito il risultato.
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {[
              {
                title: "Riassunto generico",
                desc: "Può sembrare veloce, ma spesso non ti aiuta a difendere il perche di un alert o a ritrovare il passaggio utile.",
              },
              {
                title: "Revisione manuale iniziale",
                desc: "Resta indispensabile nei casi complessi, ma richiede piu tempo gia nella fase di primo filtraggio.",
              },
              {
                title: "PeriziaScan",
                desc: "Piu rapido della prima lettura manuale e piu difendibile di una sintesi generica, perche ogni punto importante resta agganciato al documento.",
              }
            ].map((item, index) => (
              <div key={index} className="section-fade rounded-3xl border border-zinc-800 bg-zinc-900/70 p-8">
                <h3 className="text-2xl font-serif font-bold text-zinc-100 mb-4">{item.title}</h3>
                <p className="text-zinc-400 leading-relaxed">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Pricing */}
      <section id="pricing" className="py-24 px-6 bg-zinc-900/30">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-4">
              Accesso e Pacchetti
            </h2>
            <p className="text-zinc-400 text-lg">
              I crediti restano il motore del consumo, ma il valore va letto prima nel metodo: evidenze di pagina, screening piu rapido e posizionamento chiaro per ogni piano.
            </p>
          </div>
          <PublicPlansGrid />

          <div className="mt-10 flex flex-col items-center gap-4 text-center">
            <p className="max-w-3xl text-sm text-zinc-500">
              Free serve a entrare senza attrito. Starter e un top-up occasionale. Solo e il piano centrale per chi analizza sul serio. Pro e pensato per uso frequente. Studio resta su offerta manuale.
            </p>
            <Button asChild variant="outline" className="border-gold/30 text-gold hover:bg-gold/10 px-6">
              <Link to="/pacchetti">Vedi dettagli pacchetti e crediti</Link>
            </Button>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-24 px-6">
        <div className="max-w-4xl mx-auto text-center">
          <Building2 className="w-16 h-16 text-gold mx-auto mb-8" />
          <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-6">
            Inizia dalla perizia, non dal rumore
          </h2>
          <p className="text-zinc-400 text-lg mb-8">
            Carica una perizia, individua piu rapidamente le criticita principali e controlla i riferimenti direttamente nel report. Meno tempo perso nella ricerca iniziale, piu criterio nel pre-screening.
          </p>
          <Button 
            onClick={openLoginNotice}
            data-testid="cta-start-btn"
            className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-12 py-6 text-lg gold-glow"
          >
            Prova Free con 12 crediti
          </Button>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-zinc-800 py-12 px-6">
        <div className="max-w-7xl mx-auto flex flex-col md:flex-row items-center justify-between gap-6">
          <div className="flex items-center gap-3">
            <Scale className="w-6 h-6 text-gold" />
            <span className="font-serif font-bold text-zinc-100">Nexodify</span>
          </div>
          <div className="flex flex-wrap items-center justify-center gap-4 text-sm text-zinc-500">
            {[
              { label: 'Pacchetti', to: '/pacchetti' },
              { label: 'Supporto', to: '/supporto' },
              { label: 'Termini', to: '/termini' },
              { label: 'Privacy', to: '/privacy' }
            ].map((item) => (
              <Link
                key={item.to}
                to={item.to}
                className="px-3 py-1 rounded-full border border-zinc-800 bg-zinc-900/50 text-zinc-500 transition-colors hover:text-zinc-100 hover:border-zinc-700"
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

      <PreLoginNoticeDialog
        open={isLoginNoticeOpen}
        onOpenChange={setIsLoginNoticeOpen}
        onConfirm={handleConfirmLogin}
      />
    </div>
  );
};

export default Landing;
