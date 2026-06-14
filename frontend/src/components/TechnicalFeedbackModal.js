import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { toast } from 'sonner';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from './ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from './ui/select';
import { Button } from './ui/button';
import { Textarea } from './ui/textarea';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Checkbox } from './ui/checkbox';
import { CheckCircle2 } from 'lucide-react';

const API_URL = process.env.REACT_APP_BACKEND_URL;

export const FEEDBACK_LEVELS = [
  { value: 'report', label: 'Report completo' },
  { value: 'section', label: 'Sezione' },
  { value: 'specific_item', label: 'Elemento specifico' },
  { value: 'extracted_field', label: 'Dato estratto' },
  { value: 'page_reference', label: 'Riferimento di pagina' },
  { value: 'pdf_output', label: 'Output PDF' },
];

export const SECTION_KEYS = [
  { value: 'panoramica_lotto', label: 'Panoramica lotto' },
  { value: 'dettagli', label: 'Dettagli' },
  { value: 'costi_oneri', label: 'Costi e oneri' },
  { value: 'rischi_punti_critici', label: 'Rischi e punti critici' },
  { value: 'red_flags', label: 'Red flags' },
  { value: 'occupazione', label: 'Occupazione' },
  { value: 'urbanistica_catastale', label: 'Urbanistica e catastale' },
  { value: 'formalita', label: 'Formalità' },
  { value: 'quote_diritto_reale', label: 'Quote e diritto reale' },
  { value: 'superficie', label: 'Superficie' },
  { value: 'decisione_rapida', label: 'Decisione rapida' },
  { value: 'pdf_finale', label: 'PDF finale' },
  { value: 'altro', label: 'Altro' },
];

export const FEEDBACK_TYPES = [
  { value: 'corretto', label: 'Corretto' },
  { value: 'parzialmente_corretto', label: 'Parzialmente corretto' },
  { value: 'sbagliato', label: 'Sbagliato' },
  { value: 'manca_informazione', label: 'Manca informazione' },
  { value: 'classificazione_troppo_forte', label: 'Classificazione troppo forte' },
  { value: 'classificazione_troppo_debole', label: 'Classificazione troppo debole' },
  { value: 'duplicato', label: 'Duplicato' },
  { value: 'non_utile', label: 'Non utile' },
  { value: 'fonte_pagina_errata', label: 'Fonte / pagina errata' },
  { value: 'valore_estratto_errato', label: 'Valore estratto errato' },
  { value: 'wording_confuso', label: 'Wording confuso' },
  { value: 'altro', label: 'Altro' },
];

export const PRIORITIES = [
  { value: 'bassa', label: 'Bassa' },
  { value: 'media', label: 'Media' },
  { value: 'alta', label: 'Alta' },
  { value: 'bloccante', label: 'Bloccante' },
];

export const EXPECTED_CLASSIFICATIONS = [
  { value: 'fatto_rilevato', label: 'Fatto rilevato' },
  { value: 'punto_di_attenzione', label: 'Punto di attenzione' },
  { value: 'rischio_da_verificare', label: 'Rischio da verificare' },
  { value: 'blocco', label: 'Blocco' },
  { value: 'non_applicabile', label: 'Non applicabile' },
  { value: 'non_so', label: 'Non so' },
];

export const CONFIDENCE_LEVELS = [
  { value: 'sicuro', label: 'Sicuro' },
  { value: 'abbastanza_sicuro', label: 'Abbastanza sicuro' },
  { value: 'da_verificare', label: 'Da verificare' },
];

const fieldLabel = 'text-xs font-mono uppercase tracking-wider text-zinc-500 mb-1.5 block';
const selectTriggerClass = 'bg-zinc-950 border-zinc-700 text-zinc-100';

const DarkSelect = ({ value, onValueChange, placeholder, options, id }) => (
  <Select value={value} onValueChange={onValueChange}>
    <SelectTrigger id={id} className={selectTriggerClass} data-testid={id}>
      <SelectValue placeholder={placeholder} />
    </SelectTrigger>
    <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100 max-h-72">
      {options.map((opt) => (
        <SelectItem key={opt.value} value={opt.value} className="text-zinc-200 focus:bg-zinc-800 focus:text-zinc-100">
          {opt.label}
        </SelectItem>
      ))}
    </SelectContent>
  </Select>
);

/**
 * Reusable technical feedback modal for beta partners / admins.
 * Props:
 *  - open, onOpenChange
 *  - analysisId, caseId, fileName, documentHash
 *  - prefill: { sectionKey, feedbackLevel, itemReference, originalAiOutput }
 *  - betaPartnerDefaults: { permissionDefaultChecked }
 *  - onSubmitted(feedback)
 */
const TechnicalFeedbackModal = ({
  open,
  onOpenChange,
  analysisId,
  caseId,
  fileName,
  documentHash,
  prefill = {},
  permissionDefaultChecked = true,
  onSubmitted,
}) => {
  const [feedbackLevel, setFeedbackLevel] = useState('report');
  const [sectionKey, setSectionKey] = useState('altro');
  const [feedbackType, setFeedbackType] = useState('');
  const [priority, setPriority] = useState('media');
  const [expertComment, setExpertComment] = useState('');
  const [expectedCorrection, setExpectedCorrection] = useState('');
  const [expectedClassification, setExpectedClassification] = useState('');
  const [confidence, setConfidence] = useState('');
  const [pageReference, setPageReference] = useState('');
  const [permission, setPermission] = useState(permissionDefaultChecked);
  const [submitting, setSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);

  // Apply prefill when the modal opens.
  useEffect(() => {
    if (!open) return;
    setFeedbackLevel(prefill.feedbackLevel || 'report');
    setSectionKey(prefill.sectionKey || 'altro');
    setFeedbackType('');
    setPriority('media');
    setExpertComment('');
    setExpectedCorrection('');
    setExpectedClassification('');
    setConfidence('');
    setPageReference(prefill.itemReference?.page_reference != null ? String(prefill.itemReference.page_reference) : '');
    setPermission(permissionDefaultChecked);
    setSuccess(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const handleSubmit = async () => {
    if (!feedbackType) {
      toast.error('Seleziona il tipo di valutazione');
      return;
    }
    if (!expertComment.trim()) {
      toast.error('Inserisci l’osservazione tecnica');
      return;
    }
    setSubmitting(true);
    try {
      const body = {
        analysis_id: analysisId || null,
        case_id: caseId || null,
        file_name: fileName || null,
        document_hash: documentHash || null,
        feedback_level: feedbackLevel,
        section_key: sectionKey,
        feedback_type: feedbackType,
        priority,
        expert_comment: expertComment,
        expected_correction: expectedCorrection || null,
        expected_classification: expectedClassification || null,
        expert_confidence: confidence || null,
        page_reference: pageReference || null,
        permission_for_learning: permission,
        source: analysisId ? 'report_feedback_modal' : 'beta_dashboard',
        item_reference: prefill.itemReference || null,
        original_ai_output: prefill.originalAiOutput || null,
      };
      const res = await axios.post(`${API_URL}/api/beta-feedback`, body, { withCredentials: true });
      setSuccess(true);
      toast.success('Valutazione registrata');
      if (onSubmitted) onSubmitted(res.data?.feedback);
    } catch (err) {
      const detail = err?.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Errore durante l’invio della valutazione');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="bg-zinc-900 border-zinc-800 text-zinc-100 max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-zinc-100 font-serif text-xl">Valutazione tecnica del report</DialogTitle>
          <DialogDescription className="text-zinc-400">
            Segnali qui eventuali correzioni, integrazioni o osservazioni professionali sulla sezione
            analizzata. Ogni nota rester&agrave; collegata al report e ci aiuter&agrave; a migliorare
            PeriziaScan in modo concreto.
          </DialogDescription>
        </DialogHeader>

        {success ? (
          <div className="py-8 text-center" data-testid="beta-feedback-success">
            <CheckCircle2 className="w-12 h-12 text-emerald-400 mx-auto mb-4" />
            <p className="text-zinc-200 text-base mb-6 max-w-md mx-auto">
              Grazie, valutazione registrata. Il suo contributo aiuter&agrave; a migliorare PeriziaScan in modo concreto.
            </p>
            <div className="flex justify-center gap-3">
              <Button variant="outline" className="border-zinc-700 text-zinc-300 hover:bg-zinc-800" onClick={() => setSuccess(false)}>
                Aggiungi un’altra nota
              </Button>
              <Button className="bg-zinc-100 text-zinc-950 hover:bg-zinc-200" onClick={() => onOpenChange(false)}>
                Chiudi
              </Button>
            </div>
          </div>
        ) : (
          <div className="space-y-4 py-2">
            {prefill.itemReference?.item_title && (
              <div className="rounded-lg border border-zinc-800 bg-zinc-950/60 p-3 text-sm">
                <p className="text-[11px] font-mono uppercase tracking-wider text-zinc-500 mb-1">Elemento collegato</p>
                <p className="text-zinc-200">{prefill.itemReference.item_title}</p>
                {prefill.itemReference.item_path && (
                  <p className="text-xs text-zinc-500 font-mono mt-1 break-all">{prefill.itemReference.item_path}</p>
                )}
              </div>
            )}

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div>
                <Label className={fieldLabel}>Livello</Label>
                <DarkSelect id="beta-fb-level" value={feedbackLevel} onValueChange={setFeedbackLevel} placeholder="Livello" options={FEEDBACK_LEVELS} />
              </div>
              <div>
                <Label className={fieldLabel}>Sezione</Label>
                <DarkSelect id="beta-fb-section" value={sectionKey} onValueChange={setSectionKey} placeholder="Sezione" options={SECTION_KEYS} />
              </div>
              <div>
                <Label className={fieldLabel}>Tipo di valutazione *</Label>
                <DarkSelect id="beta-fb-type" value={feedbackType} onValueChange={setFeedbackType} placeholder="Seleziona" options={FEEDBACK_TYPES} />
              </div>
              <div>
                <Label className={fieldLabel}>Priorit&agrave;</Label>
                <DarkSelect id="beta-fb-priority" value={priority} onValueChange={setPriority} placeholder="Priorità" options={PRIORITIES} />
              </div>
            </div>

            <div>
              <Label className={fieldLabel} htmlFor="beta-fb-comment">Osservazione tecnica *</Label>
              <Textarea
                id="beta-fb-comment"
                data-testid="beta-fb-comment"
                value={expertComment}
                onChange={(e) => setExpertComment(e.target.value)}
                placeholder="Indichi cosa correggere, integrare o verificare dal punto di vista tecnico."
                className="bg-zinc-950 border-zinc-700 text-zinc-100 min-h-[96px]"
                maxLength={5000}
              />
            </div>

            <div>
              <Label className={fieldLabel} htmlFor="beta-fb-correction">Correzione suggerita</Label>
              <Textarea
                id="beta-fb-correction"
                value={expectedCorrection}
                onChange={(e) => setExpectedCorrection(e.target.value)}
                placeholder="Se possibile, indichi come dovrebbe essere formulato o classificato correttamente."
                className="bg-zinc-950 border-zinc-700 text-zinc-100 min-h-[72px]"
                maxLength={5000}
              />
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div>
                <Label className={fieldLabel}>Classificazione corretta</Label>
                <DarkSelect id="beta-fb-class" value={expectedClassification} onValueChange={setExpectedClassification} placeholder="Opzionale" options={EXPECTED_CLASSIFICATIONS} />
              </div>
              <div>
                <Label className={fieldLabel}>Livello di sicurezza</Label>
                <DarkSelect id="beta-fb-confidence" value={confidence} onValueChange={setConfidence} placeholder="Opzionale" options={CONFIDENCE_LEVELS} />
              </div>
              <div>
                <Label className={fieldLabel} htmlFor="beta-fb-page">Pagina della perizia</Label>
                <Input
                  id="beta-fb-page"
                  value={pageReference}
                  onChange={(e) => setPageReference(e.target.value)}
                  placeholder="es. 12"
                  className="bg-zinc-950 border-zinc-700 text-zinc-100"
                  maxLength={64}
                />
              </div>
            </div>

            <label className="flex items-start gap-3 rounded-lg border border-zinc-800 bg-zinc-950/60 p-3 cursor-pointer">
              <Checkbox checked={permission} onCheckedChange={(v) => setPermission(Boolean(v))} className="mt-0.5 border-zinc-600 data-[state=checked]:bg-gold data-[state=checked]:border-gold" />
              <span className="text-sm text-zinc-300">
                Autorizzo l&rsquo;uso di questo feedback per migliorare PeriziaScan in forma interna e non pubblica.
              </span>
            </label>

            <div className="flex flex-col-reverse gap-3 sm:flex-row sm:justify-end pt-2">
              <Button variant="outline" className="border-zinc-700 text-zinc-300 hover:bg-zinc-800" onClick={() => onOpenChange(false)} disabled={submitting}>
                Annulla
              </Button>
              <Button
                className="bg-gold text-zinc-950 hover:bg-gold/90"
                onClick={handleSubmit}
                disabled={submitting}
                data-testid="beta-fb-submit"
              >
                {submitting ? 'Invio in corso…' : 'Invia valutazione'}
              </Button>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
};

export default TechnicalFeedbackModal;
