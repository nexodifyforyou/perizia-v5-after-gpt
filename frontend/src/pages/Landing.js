import React, { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Button } from '../components/ui/button';
import PreLoginNoticeDialog from '../components/PreLoginNoticeDialog';
import { 
  FileText, 
  Shield, 
  Eye, 
  MessageSquare, 
  ArrowRight,
  Scale,
  Building2,
  Calculator,
  AlertTriangle,
  Sparkles
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
      title: "Perizia Analysis",
      title_it: "Analisi della Perizia",
      description: "Structured appraisal analysis with traceable references",
      description_it: "Lettura strutturata della perizia con conclusioni ancorate al documento e riferimenti verificabili"
    },
    {
      icon: <Shield className="w-6 h-6" />,
      title: "Legal Killers Check",
      title_it: "Criticità Legali",
      description: "Detection of legal blockers tied to the source document",
      description_it: "Individuazione delle criticità legali da verificare, con indicazioni collegate alla perizia"
    },
    {
      icon: <Calculator className="w-6 h-6" />,
      title: "Money Box Calculator",
      title_it: "Costi e Oneri",
      description: "Transparent cost breakdown where the document supports it",
      description_it: "Quadro dei costi e degli oneri da verificare, con calcoli trasparenti dove la perizia lo consente"
    },
    {
      icon: <Eye className="w-6 h-6" />,
      title: "Image Forensics",
      title_it: "Image Forensics",
      description: "Image review capabilities currently evolving",
      description_it: "Funzionalita in evoluzione per l'analisi delle immagini dell'immobile"
    },
    {
      icon: <MessageSquare className="w-6 h-6" />,
      title: "AI Assistant",
      title_it: "Assistente sulla Perizia",
      description: "Guided Q&A capabilities currently evolving",
      description_it: "Funzionalita in evoluzione per domande guidate sulla perizia e sui punti da approfondire"
    },
    {
      icon: <AlertTriangle className="w-6 h-6" />,
      title: "Semaforo Risk System",
      title_it: "Semaforo di Rischio",
      description: "A fast risk-oriented view for initial screening",
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
            <span className="text-sm font-mono text-gold">Logica proprietaria, verifiche ancorate alla perizia</span>
          </div>
          
          <h1 className="text-5xl md:text-7xl font-serif font-bold text-zinc-100 tracking-tight leading-none mb-6">
            Analisi strutturata della perizia.
          </h1>
          
          <p className="text-lg md:text-xl text-zinc-400 max-w-3xl mx-auto mb-12 leading-relaxed">
            Con focus iniziale sulle perizie d’asta, PeriziaScan trasforma documenti lunghi e opachi in una lettura chiara e tracciabile: semaforo rischio, criticità legali, costi/oneri da verificare e riferimenti puntuali alle pagine.
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Button 
              onClick={openLoginNotice}
              data-testid="hero-get-started-btn"
              className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-8 py-6 text-lg gold-glow gold-glow-hover"
            >
              Carica la tua prima perizia <ArrowRight className="w-5 h-5 ml-2" />
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

      {/* Semaforo Preview */}
      <section className="py-16 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-8">
            <p className="text-zinc-500 text-sm md:text-base">
              Una vista immediata per capire dove concentrare le verifiche iniziali.
            </p>
          </div>
          <div className="grid grid-cols-3 gap-4 max-w-lg mx-auto">
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

      {/* Features */}
      <section id="features" className="py-24 px-6 bg-zinc-900/30">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-4">
              Cosa trovi nel report
            </h2>
            <p className="text-zinc-400 text-lg">
              Un'analisi iniziale piu controllabile, costruita per evidenziare i punti da verificare nella perizia
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
                  {(feature.title_it === 'Image Forensics' || feature.title_it === 'Assistente sulla Perizia') && (
                    <span className="block text-xs font-mono text-zinc-500 mt-2 uppercase tracking-wider">In arrivo</span>
                  )}
                </h3>
                <p className="text-zinc-400 text-sm leading-relaxed">{feature.description_it}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* How It Works */}
      <section className="py-24 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-4">
              Come Funziona
            </h2>
            <p className="text-zinc-400 text-lg">
              Un flusso pensato per ridurre il lavoro manuale di lettura e ricerca, senza trasformare la verifica in una black box
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
            {[
              { step: "01", title: "Carica la Perizia", desc: "Importa il PDF della perizia per avviare il pre-screening documentale" },
              { step: "02", title: "Strutturazione dei Dati", desc: "Il sistema organizza le informazioni rilevanti e i punti da controllare" },
              { step: "03", title: "Rischi, Criticita e Costi", desc: "Ricevi una sintesi strutturata con semaforo, criticita legali e oneri da verificare" },
              { step: "04", title: "Controllo del Report", desc: "Verifica riferimenti di pagina e conclusioni supportate, dove applicabile, da calcoli trasparenti" }
            ].map((item, index) => (
              <div key={index} className="text-center">
                <div className="text-6xl font-serif font-bold text-gold/20 mb-4">{item.step}</div>
                <h3 className="text-xl font-semibold text-zinc-100 mb-2">{item.title}</h3>
                <p className="text-zinc-400 text-sm">{item.desc}</p>
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
              Una sintesi commerciale pulita dei pacchetti pubblici. Il dettaglio completo di crediti, validita e perimetro del prodotto attuale e disponibile nella pagina Pacchetti.
            </p>
          </div>
          <PublicPlansGrid />

          <div className="mt-10 flex flex-col items-center gap-4 text-center">
            <p className="max-w-3xl text-sm text-zinc-500">
              Il valore attuale dei pacchetti riguarda il core product documentale. Assistente sulla Perizia e Image Forensics non sono inclusi oggi nel valore attivo dei pacchetti.
            </p>
            <Button asChild variant="outline" className="border-gold/30 text-gold hover:bg-gold/10 px-6">
              <Link to="/pacchetti">Vedi dettagli pacchetti e crediti</Link>
            </Button>
          </div>
        </div>
      </section>

      {/* In Arrivo */}
      <section className="py-24 px-6">
        <div className="max-w-5xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-4">
              In Arrivo
            </h2>
            <p className="text-zinc-400 text-lg max-w-3xl mx-auto">
              Alcune funzionalita sono gia presenti in forma iniziale, ma non rappresentano oggi il nucleo principale della proposta PeriziaScan.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-zinc-900/60 border border-zinc-800 rounded-2xl p-8">
              <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-zinc-800 text-zinc-400 text-xs font-mono uppercase tracking-wider mb-4">
                Prossimamente
              </div>
              <h3 className="text-2xl font-serif font-bold text-zinc-100 mb-3">
                Assistente sulla Perizia
              </h3>
              <p className="text-zinc-400 leading-relaxed">
                Supporto conversazionale per interrogare il contenuto della perizia e approfondire i punti che richiedono controllo professionale.
              </p>
            </div>

            <div className="bg-zinc-900/60 border border-zinc-800 rounded-2xl p-8">
              <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-zinc-800 text-zinc-400 text-xs font-mono uppercase tracking-wider mb-4">
                Prossimamente
              </div>
              <h3 className="text-2xl font-serif font-bold text-zinc-100 mb-3">
                Image Forensics sulle Immagini Immobile
              </h3>
              <p className="text-zinc-400 leading-relaxed">
                Funzionalita dedicate alla lettura delle immagini dell'immobile per affiancare l'analisi documentale, senza sostituire il sopralluogo o la verifica tecnica.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-24 px-6">
        <div className="max-w-4xl mx-auto text-center">
          <Building2 className="w-16 h-16 text-gold mx-auto mb-8" />
          <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-6">
            Inizia dalla perizia, non dalle ipotesi
          </h2>
          <p className="text-zinc-400 text-lg mb-8">
            Carica una perizia, individua piu rapidamente le criticita principali e controlla i riferimenti direttamente nel report. Meno tempo perso nella ricerca iniziale, piu efficienza nel pre-screening.
          </p>
          <Button 
            onClick={openLoginNotice}
            data-testid="cta-start-btn"
            className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-12 py-6 text-lg gold-glow"
          >
            Avvia l'analisi della perizia
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
