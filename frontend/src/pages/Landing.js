import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Button } from '../components/ui/button';
import { 
  FileText, 
  Shield, 
  Eye, 
  MessageSquare, 
  CheckCircle2, 
  ArrowRight,
  Scale,
  Building2,
  Calculator,
  AlertTriangle,
  Sparkles
} from 'lucide-react';
import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const Landing = () => {
  const { user, login } = useAuth();
  const navigate = useNavigate();
  const [plans, setPlans] = useState([]);

  useEffect(() => {
    if (user) {
      navigate('/dashboard');
    }
    fetchPlans();
  }, [user, navigate]);

  const fetchPlans = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/plans`);
      setPlans(response.data.plans);
    } catch (error) {
      console.error('Failed to fetch plans:', error);
    }
  };

  const features = [
    {
      icon: <FileText className="w-6 h-6" />,
      title: "Perizia Analysis",
      title_it: "Analisi Perizia",
      description: "AI-powered forensic analysis of CTU documents with evidence tracking",
      description_it: "Analisi forense AI dei documenti CTU con tracciamento evidenze"
    },
    {
      icon: <Shield className="w-6 h-6" />,
      title: "Legal Killers Check",
      title_it: "Verifica Legal Killers",
      description: "Automated detection of 8 critical legal issues that can kill a deal",
      description_it: "Rilevamento automatico di 8 criticità legali che possono bloccare un affare"
    },
    {
      icon: <Calculator className="w-6 h-6" />,
      title: "Money Box Calculator",
      title_it: "Calcolatore Costi",
      description: "Complete cost breakdown from regularization to liberation",
      description_it: "Breakdown completo dei costi dalla regolarizzazione alla liberazione"
    },
    {
      icon: <Eye className="w-6 h-6" />,
      title: "Image Forensics",
      title_it: "Forensics Immagini",
      description: "Visual defect detection and material analysis from site photos",
      description_it: "Rilevamento difetti visivi e analisi materiali da foto del sito"
    },
    {
      icon: <MessageSquare className="w-6 h-6" />,
      title: "AI Assistant",
      title_it: "Assistente AI",
      description: "Expert Q&A on Italian real estate auctions and documents",
      description_it: "Q&A esperto su aste immobiliari italiane e documenti"
    },
    {
      icon: <AlertTriangle className="w-6 h-6" />,
      title: "Semaforo Risk System",
      title_it: "Sistema Semaforo Rischio",
      description: "Instant RED/AMBER/GREEN risk assessment at a glance",
      description_it: "Valutazione rischio istantanea ROSSO/GIALLO/VERDE"
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
            onClick={login}
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
            <span className="text-sm font-mono text-gold">Audit-Grade Forensic Analysis</span>
          </div>
          
          <h1 className="text-5xl md:text-7xl font-serif font-bold text-zinc-100 tracking-tight leading-none mb-6">
            Forensic Engine for
            <br />
            <span className="text-gold">Italian Real Estate</span>
          </h1>
          
          <p className="text-lg md:text-xl text-zinc-400 max-w-3xl mx-auto mb-12 leading-relaxed">
            Analisi deterministica e audit-grade di perizie CTU, foto del sito e documentazione. 
            <br className="hidden md:block" />
            Decisioni informate per aste immobiliari italiane.
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Button 
              onClick={login}
              data-testid="hero-get-started-btn"
              className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-8 py-6 text-lg gold-glow gold-glow-hover"
            >
              Inizia Gratis <ArrowRight className="w-5 h-5 ml-2" />
            </Button>
            <Button 
              variant="outline"
              data-testid="hero-learn-more-btn"
              className="border-zinc-700 text-zinc-300 hover:bg-zinc-800 px-8 py-6 text-lg"
              onClick={() => document.getElementById('features').scrollIntoView({ behavior: 'smooth' })}
            >
              Scopri di più
            </Button>
          </div>
        </div>
      </section>

      {/* Semaforo Preview */}
      <section className="py-16 px-6">
        <div className="max-w-7xl mx-auto">
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
              Analisi Forense Completa
            </h2>
            <p className="text-zinc-400 text-lg">
              Ogni strumento necessario per valutare un immobile all'asta
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
                <h3 className="text-xl font-semibold text-zinc-100 mb-2">{feature.title_it}</h3>
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
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
            {[
              { step: "01", title: "Carica PDF", desc: "Carica la perizia CTU in formato PDF" },
              { step: "02", title: "Analisi AI", desc: "Il nostro engine analizza ogni pagina" },
              { step: "03", title: "Report Forense", desc: "Ricevi report dettagliato con evidenze" },
              { step: "04", title: "Decisione", desc: "Sistema semaforo per decisioni rapide" }
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
              Piani e Prezzi
            </h2>
            <p className="text-zinc-400 text-lg">
              Scegli il piano più adatto alle tue esigenze
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 max-w-5xl mx-auto">
            {plans.map((plan, index) => (
              <div 
                key={plan.plan_id}
                data-testid={`plan-card-${plan.plan_id}`}
                className={`relative bg-zinc-900 border rounded-2xl p-8 transition-all duration-300 ${
                  plan.plan_id === 'pro' 
                    ? 'border-gold gold-glow scale-105' 
                    : 'border-zinc-800 hover:border-zinc-600'
                }`}
              >
                {plan.plan_id === 'pro' && (
                  <div className="absolute -top-4 left-1/2 -translate-x-1/2">
                    <span className="premium-badge">Più Popolare</span>
                  </div>
                )}
                
                <h3 className="text-2xl font-serif font-bold text-zinc-100 mb-2">
                  {plan.name_it}
                </h3>
                
                <div className="flex items-baseline gap-1 mb-6">
                  <span className="text-4xl font-bold text-gold">
                    €{plan.price.toFixed(0)}
                  </span>
                  {plan.price > 0 && (
                    <span className="text-zinc-500">/mese</span>
                  )}
                </div>
                
                <ul className="space-y-3 mb-8">
                  {plan.features_it.map((feature, i) => (
                    <li key={i} className="flex items-start gap-3 text-sm text-zinc-300">
                      <CheckCircle2 className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                      {feature}
                    </li>
                  ))}
                </ul>
                
                <Button 
                  onClick={login}
                  data-testid={`plan-${plan.plan_id}-btn`}
                  className={`w-full ${
                    plan.plan_id === 'pro'
                      ? 'bg-gold text-zinc-950 hover:bg-gold-dim'
                      : 'bg-zinc-800 text-zinc-100 hover:bg-zinc-700'
                  }`}
                >
                  {plan.price === 0 ? 'Inizia Gratis' : 'Abbonati'}
                </Button>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-24 px-6">
        <div className="max-w-4xl mx-auto text-center">
          <Building2 className="w-16 h-16 text-gold mx-auto mb-8" />
          <h2 className="text-3xl md:text-5xl font-serif font-bold text-zinc-100 mb-6">
            Pronto per Analizzare la Tua Prima Perizia?
          </h2>
          <p className="text-zinc-400 text-lg mb-8">
            Unisciti a centinaia di investitori e professionisti che usano Nexodify per decisioni più informate
          </p>
          <Button 
            onClick={login}
            data-testid="cta-start-btn"
            className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-12 py-6 text-lg gold-glow"
          >
            Inizia Ora - È Gratis
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
          <p className="text-zinc-500 text-sm">
            © 2025 Nexodify Forensic Engine. Documento informativo, non è consulenza legale.
          </p>
        </div>
      </footer>
    </div>
  );
};

export default Landing;
