import React, { useEffect, useMemo, useState } from 'react';
import { FileText, Quote, Search, X } from 'lucide-react';
import { Button } from './ui/button';
import { patchPeriziaHeadline } from '../lib/api/perizia';

const FIELD_LABELS = {
  tribunale: 'Tribunale',
  procedura: 'Procedura',
  lotto: 'Lotto',
  address: 'Indirizzo'
};

const FIELD_PLACEHOLDERS = {
  tribunale: 'Es. TRIBUNALE DI MANTOVA',
  procedura: 'Es. Esecuzione Immobiliare 62/2024 R.G.E.',
  lotto: 'Es. Lotto Unico',
  address: 'Es. Via Sordello 5, San Giorgio Bigarello (MN)'
};

const normalizeArray = (value) => {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  return [value];
};

const buildEvidenceLine = (item, fallbackLabel) => {
  if (!item) return null;
  const page = item.page ? `Pag. ${item.page}` : null;
  const text = item.quote || item.snippet || item.keyword || '';
  if (!page && !text) return null;
  if (page && text) return `${page} — ${text}`;
  return page || text || fallbackLabel;
};

const HeadlineVerifyModal = ({
  open,
  onClose,
  analysisId,
  fieldKey,
  fieldState,
  currentDisplayValue,
  onSaved
}) => {
  const [value, setValue] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!open) return;
    const initial = typeof fieldState?.value === 'string' ? fieldState.value : '';
    setValue(initial);
    setError('');
  }, [open, fieldState]);

  const status = fieldState?.status;
  const label = FIELD_LABELS[fieldKey] || 'Dato';
  const placeholder = FIELD_PLACEHOLDERS[fieldKey] || 'Inserisci valore';

  const evidenceLines = useMemo(() => {
    return normalizeArray(fieldState?.evidence)
      .map((item) => buildEvidenceLine(item, 'Estratto disponibile'))
      .filter(Boolean)
      .slice(0, 3);
  }, [fieldState]);

  const searchedLines = useMemo(() => {
    return normalizeArray(fieldState?.searched_in)
      .map((item) => buildEvidenceLine(item, 'Ricerca disponibile'))
      .filter(Boolean)
      .slice(0, 5);
  }, [fieldState]);

  const canSave = value.trim().length > 0 && !saving;

  const handleSave = async () => {
    if (!canSave) return;
    setSaving(true);
    setError('');
    try {
      await patchPeriziaHeadline(analysisId, { [fieldKey]: value.trim() });
      if (onSaved) {
        await onSaved();
      }
      onClose();
    } catch (err) {
      setError('Errore nel salvataggio. Riprova.');
    } finally {
      setSaving(false);
    }
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/70" onClick={onClose} />
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 max-w-lg w-full mx-4 shadow-xl">
        <button
          onClick={onClose}
          className="absolute top-4 right-4 text-zinc-500 hover:text-zinc-300"
          aria-label="Chiudi"
        >
          <X className="w-5 h-5" />
        </button>

        <div className="flex items-start gap-3 mb-4">
          <div className="p-2 bg-amber-500/20 rounded-lg">
            <HelpIcon status={status} />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-zinc-100">Verifica {label}</h3>
            <p className="text-sm text-zinc-400 mt-1">
              {status === 'LOW_CONFIDENCE'
                ? 'Dato estratto con bassa confidenza. Verifica in perizia e conferma.'
                : 'Dato non trovato nella perizia. Inseriscilo manualmente se presente.'}
            </p>
          </div>
        </div>

        {fieldState?.user_prompt_it && (
          <div className="mb-4 p-3 bg-zinc-950 border border-zinc-800 rounded-lg text-sm text-zinc-300">
            {fieldState.user_prompt_it}
          </div>
        )}

        <div className="space-y-4">
          {(evidenceLines.length > 0 || searchedLines.length > 0) && (
            <div className="p-3 bg-zinc-950 border border-zinc-800 rounded-lg">
              <p className="text-xs font-mono text-zinc-500 mb-2">Evidenze / Ricerca</p>
              <div className="space-y-2">
                {evidenceLines.length > 0 && (
                  <div className="space-y-1">
                    {evidenceLines.map((line, idx) => (
                      <div key={`evidence-${idx}`} className="flex items-start gap-2 text-xs text-zinc-300">
                        <Quote className="w-4 h-4 text-gold flex-shrink-0 mt-0.5" />
                        <span>{line}</span>
                      </div>
                    ))}
                  </div>
                )}
                {evidenceLines.length === 0 && searchedLines.length > 0 && (
                  <div className="space-y-1">
                    {searchedLines.map((line, idx) => (
                      <div key={`search-${idx}`} className="flex items-start gap-2 text-xs text-zinc-400">
                        <Search className="w-4 h-4 text-zinc-500 flex-shrink-0 mt-0.5" />
                        <span>{line}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          <div>
            <label className="text-xs font-mono text-zinc-500">Valore corretto</label>
            <input
              type="text"
              value={value}
              onChange={(event) => setValue(event.target.value)}
              placeholder={placeholder}
              className="mt-2 w-full bg-zinc-950 border border-zinc-700 rounded-lg px-3 py-2 text-zinc-100 focus:border-gold focus:outline-none"
            />
            {currentDisplayValue && (
              <p className="text-xs text-zinc-500 mt-2">
                Valore attuale: <span className="text-zinc-300">{currentDisplayValue}</span>
              </p>
            )}
          </div>
        </div>

        {error && (
          <p className="text-sm text-red-400 mt-4">{error}</p>
        )}

        <div className="mt-6 flex justify-end gap-2">
          <Button variant="outline" onClick={onClose} className="border-zinc-700 text-zinc-300">
            Annulla
          </Button>
          <Button
            onClick={handleSave}
            disabled={!canSave}
            className="bg-gold text-zinc-950 hover:bg-gold-dim disabled:opacity-50"
          >
            {saving ? 'Salvataggio...' : 'Salva'}
          </Button>
        </div>
      </div>
    </div>
  );
};

const HelpIcon = ({ status }) => {
  if (status === 'LOW_CONFIDENCE') {
    return <FileText className="w-5 h-5 text-amber-300" />;
  }
  return <Search className="w-5 h-5 text-amber-300" />;
};

export default HeadlineVerifyModal;
