import React, { useEffect, useState } from 'react';
import { Link, useLocation, useParams } from 'react-router-dom';
import axios from 'axios';
import { ArrowLeft, FileText } from 'lucide-react';
import { buildPeriziaPrintReportModel, normalizeAnalysisResponse, summarizeEvidence } from '../lib/periziaPrintModel';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const EvidenceFootnote = ({ evidence, showQuote = true }) => {
  const summary = summarizeEvidence(evidence);
  if (!summary.pages && !summary.quote) return null;
  return (
    <div className="print-evidence">
      {summary.pages ? <span className="print-evidence-pages">{summary.pages}</span> : null}
      {showQuote && summary.quote ? <p className="print-evidence-quote">"{summary.quote}"</p> : null}
    </div>
  );
};

const MetricCard = ({ label, value }) => (
  <div className="print-metric-card">
    <p className="print-card-label">{label}</p>
    <p className="print-metric-value">{value}</p>
  </div>
);

const DetailBlock = ({ title, items }) => {
  const filtered = Array.isArray(items) ? items.filter((item) => item?.value) : [];
  if (filtered.length === 0) return null;
  return (
    <div className="print-subgrid print-detail-block">
      <h4 className="print-subtitle">{title}</h4>
      <div className="print-card-grid two">
        {filtered.map((item) => (
          <div key={`${title}-${item.label}`} className="print-info-card">
            <p className="print-card-label">{item.label}</p>
            <p className="print-card-value">{item.value}</p>
            {item.note ? <p className="print-card-subvalue">{item.note}</p> : null}
            <EvidenceFootnote evidence={item.evidence} />
          </div>
        ))}
      </div>
    </div>
  );
};

const AnalysisPrintView = () => {
  const { analysisId } = useParams();
  const location = useLocation();
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    document.body.classList.add('perizia-print-body');
    document.documentElement.classList.add('perizia-print-html');
    return () => {
      document.body.classList.remove('perizia-print-body');
      document.documentElement.classList.remove('perizia-print-html');
    };
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const useDebugPayload = params.get('debug') === '1' && typeof window !== 'undefined' && window.__DEBUG_ANALYSIS_PAYLOAD__;
    const fetchAnalysis = async () => {
      try {
        if (useDebugPayload) {
          setAnalysis(normalizeAnalysisResponse(window.__DEBUG_ANALYSIS_PAYLOAD__));
          return;
        }
        const response = await axios.get(`${API_URL}/api/analysis/perizia/${analysisId}`, {
          withCredentials: true,
        });
        setAnalysis(normalizeAnalysisResponse(response.data));
      } catch (err) {
        setError("Impossibile caricare l'analisi per la vista stampa.");
      } finally {
        setLoading(false);
      }
    };
    fetchAnalysis();
  }, [analysisId, location.search]);

  useEffect(() => {
    if (!loading && analysis) {
      window.__PERIZIA_PRINT_READY__ = true;
      document.body.setAttribute('data-print-report-ready', 'true');
    } else {
      window.__PERIZIA_PRINT_READY__ = false;
      document.body.setAttribute('data-print-report-ready', 'false');
    }
    return () => {
      window.__PERIZIA_PRINT_READY__ = false;
      document.body.removeAttribute('data-print-report-ready');
    };
  }, [analysis, loading]);

  if (loading) {
    return (
      <div className="print-loading-shell">
        <div className="print-loading-card">
          <FileText className="w-8 h-8" />
          <p>Preparazione report di stampa...</p>
        </div>
      </div>
    );
  }

  if (error || !analysis) {
    return (
      <div className="print-loading-shell">
        <div className="print-loading-card">
          <p>{error || 'Analisi non disponibile.'}</p>
          <Link to={`/analysis/${analysisId}`} className="print-back-link">Torna all'analisi</Link>
        </div>
      </div>
    );
  }

  const model = buildPeriziaPrintReportModel(analysis);

  return (
    <div className="perizia-print-view" data-print-view="perizia-report">
      <div className="print-screen-toolbar screen-only">
        <Link to={`/analysis/${analysisId}`} className="print-back-link">
          <ArrowLeft className="w-4 h-4" />
          Torna all'analisi
        </Link>
        <button type="button" className="print-action-button" onClick={() => window.print()}>
          Stampa / Salva PDF
        </button>
      </div>

      <div className="print-report-shell">
        <article className="print-report-paper">
          <section className="print-cover">
            <div className="print-cover-brand">PeriziaScan</div>
            <div className="print-cover-grid">
              <div>
                <p className="print-kicker">Report decisionale immobiliare</p>
                <h1 className="print-title">{model.title}</h1>
                <p className="print-summary">{model.cover.summaryIt}</p>
              </div>
              <div className="print-cover-card">
                <div className="print-cover-row"><span>Documento</span><strong>{model.fileName}</strong></div>
                <div className="print-cover-row"><span>Creato il</span><strong>{model.createdAt}</strong></div>
                <div className="print-cover-row"><span>Profilo</span><strong>{model.cover.semaforo}</strong></div>
              </div>
            </div>
            <div className="print-card-grid two cover-meta">
              <div className="print-info-card"><p className="print-card-label">Procedura</p><p className="print-card-value">{model.cover.procedura}</p></div>
              <div className="print-info-card"><p className="print-card-label">Tribunale</p><p className="print-card-value">{model.cover.tribunale}</p></div>
              <div className="print-info-card"><p className="print-card-label">Lotto</p><p className="print-card-value">{model.cover.lotto}</p></div>
              <div className="print-info-card"><p className="print-card-label">Indirizzo</p><p className="print-card-value">{model.cover.indirizzo}</p></div>
            </div>
          </section>

          <section className="print-section">
            <div className="print-section-header">
              <p className="print-section-index">01</p>
              <div>
                <h2 className="print-section-title">Panoramica</h2>
                <p className="print-section-description">Sintesi dei punti decisivi per una prima valutazione dell'operazione.</p>
              </div>
            </div>
            <div className="print-card-grid two">
              <div className="print-info-card">
                <p className="print-card-label">Nota operativa</p>
                <p className="print-card-value">{model.overview.driver || "Prima dell'offerta e' consigliabile una verifica documentale mirata."}</p>
              </div>
              <div className="print-info-card">
                <p className="print-card-label">Lettura sintetica</p>
                <p className="print-card-value">{model.overview.decisionIt}</p>
              </div>
            </div>
            <div className="print-card-grid metrics">
              {model.overview.metrics.map((metric) => (
                <MetricCard key={metric.label} label={metric.label} value={metric.value} />
              ))}
            </div>
            {model.overview.composition.length > 0 ? (
              <div className="print-subgrid">
                <h3 className="print-subtitle">Composizione lotto</h3>
                <div className="print-card-grid two">
                  {model.overview.composition.map((item) => (
                    <div key={item.key} className="print-info-card compact">
                      <p className="print-card-title">{item.title}</p>
                      <p className="print-card-value">{item.type}</p>
                      <p className="print-card-subvalue">{[item.location, item.piano].filter(Boolean).join(' | ')}</p>
                      <p className="print-card-subvalue">{[item.superficie, item.valoreStima].filter(Boolean).join(' | ')}</p>
                      <EvidenceFootnote evidence={item.evidence} showQuote={false} />
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </section>

          <section className="print-section section-break">
            <div className="print-section-header">
              <p className="print-section-index">02</p>
              <div>
                <h2 className="print-section-title">Costi</h2>
                <p className="print-section-description">Quadro economico sintetico con distinzione fra perizia, stime operative e voci da approfondire.</p>
              </div>
            </div>
            <div className="print-card-grid two">
              <div className="print-info-card">
                <p className="print-card-label">Deprezzamenti da perizia</p>
                <p className="print-card-value">{model.costs.valuationAdjustments.amount}</p>
                <p className="print-card-subvalue">{model.costs.valuationAdjustments.note}</p>
                <EvidenceFootnote evidence={model.costs.valuationAdjustments.evidence} />
              </div>
              {model.costs.scenarioRange ? (
                <div className="print-info-card accent">
                  <p className="print-card-label">Scenario extra-costi stimato</p>
                  <p className="print-card-value">{model.costs.scenarioRange}</p>
                  <p className="print-card-subvalue">Range operativo basato sulle stime Nexodify, esclusi i deprezzamenti.</p>
                </div>
              ) : null}
            </div>
            <div className="print-subgrid">
              <h3 className="print-subtitle">Costi espliciti citati nel testo</h3>
                <div className="print-list">
                  {model.costs.explicitCostMentions.length > 0 ? model.costs.explicitCostMentions.map((item) => (
                  <div key={item.key} className="print-list-row">
                    <div>
                      <p className="print-card-value">{item.label}</p>
                      {item.note ? <p className="print-card-subvalue">{item.note}</p> : null}
                      <EvidenceFootnote evidence={item.evidence} showQuote={false} />
                    </div>
                    <strong>{item.amount}</strong>
                  </div>
                )) : <p className="print-empty-state">Nessun costo esplicito affidabile disponibile.</p>}
              </div>
            </div>
            <div className="print-subgrid">
              <h3 className="print-subtitle">Stime Nexodify</h3>
              <div className="print-list">
                {model.costs.nexodifyEstimateItems.length > 0 ? model.costs.nexodifyEstimateItems.map((item) => (
                  <div key={item.key} className="print-list-row">
                    <div>
                      <p className="print-card-value">{item.label}</p>
                      {item.note ? <p className="print-card-subvalue">{item.note}</p> : null}
                      <EvidenceFootnote evidence={item.evidence} showQuote={false} />
                    </div>
                    <strong>{item.amount}</strong>
                  </div>
                )) : <p className="print-empty-state">Nessuna stima Nexodify disponibile.</p>}
              </div>
            </div>
          </section>

          <section className="print-section section-break">
            <div className="print-section-header">
              <p className="print-section-index">03</p>
              <div>
                <h2 className="print-section-title">Verifiche legali prioritarie</h2>
                <p className="print-section-description">Controlli che possono incidere in modo materiale sulla convenienza o sulla fattibilita dell'acquisto.</p>
              </div>
            </div>
            <div className="print-card-grid two">
              {model.legal.length > 0 ? model.legal.map((item) => (
                <div key={item.key} className="print-info-card">
                  <div className="print-status-row">
                    <p className="print-card-title">{item.title}</p>
                    <span className="print-status-badge">{item.status}</span>
                  </div>
                  <p className="print-card-value">{item.detail}</p>
                  <EvidenceFootnote evidence={item.evidence} showQuote={false} />
                </div>
              )) : <p className="print-empty-state">Nessun blocker legale materiale disponibile.</p>}
            </div>
          </section>

          <section className="print-section section-break">
            <div className="print-section-header">
              <p className="print-section-index">04</p>
              <div>
                <h2 className="print-section-title">Dettagli per bene</h2>
                <p className="print-section-description">Lettura ordinata dei singoli beni con dati chiave, profili tecnici e riferimenti essenziali alla perizia.</p>
              </div>
            </div>
            <div className="print-detail-stack">
              {model.details.map((card) => (
                <article key={card.key} className="print-detail-card">
                  <div className="print-detail-intro">
                    <div className="print-status-row">
                      <div>
                        <h3 className="print-card-title">{card.title}</h3>
                        <p className="print-card-subvalue">{[card.location, card.piano, card.superficie, card.valoreStima].filter(Boolean).join(' | ')}</p>
                      </div>
                    </div>
                    <EvidenceFootnote evidence={card.topEvidence} showQuote={false} />
                  </div>
                  <DetailBlock title="Dati principali" items={card.detailRows} />
                  <DetailBlock title="Impianti" items={card.impiantiRows} />
                  <DetailBlock title="Certificazioni e dichiarazioni" items={card.declarationRows} />
                </article>
              ))}
            </div>
          </section>

          <section className="print-section section-break">
            <div className="print-section-header">
              <p className="print-section-index">05</p>
              <div>
                <h2 className="print-section-title">Punti di attenzione</h2>
                <p className="print-section-description">Elementi da verificare prima dell'offerta per evitare sorprese economiche o procedurali.</p>
              </div>
            </div>
            <div className="print-list">
              {model.flags.length > 0 ? model.flags.map((flag) => (
                <div key={flag.key} className="print-flag-row">
                  <div className="print-flag-meta">
                    <span className="print-status-badge">{flag.severity}</span>
                    <p className="print-card-title">{flag.title}</p>
                  </div>
                  <p className="print-card-subvalue">{flag.detail}</p>
                  <EvidenceFootnote evidence={flag.evidence} showQuote={false} />
                </div>
              )) : <p className="print-empty-state">Nessuna red flag disponibile.</p>}
            </div>
          </section>

          <footer className="print-footer">
            <p>{model.disclaimer.it}</p>
          </footer>
        </article>
      </div>
    </div>
  );
};

export default AnalysisPrintView;
