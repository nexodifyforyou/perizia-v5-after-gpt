import React, { useState } from 'react';
import { Loader2, Quote, X } from 'lucide-react';
import { Button } from '../ui/button';

// Focused confirmation panel for a single decision-model finding.
//
// Shows ONE question, the page + verbatim excerpt, 2-4 deterministic options
// plus "Non sono sicuro", and cancel/confirm. It never renders a global
// questionnaire and never invents options — every choice comes from the
// backend `finding.confirmation`. Confirm is disabled until a choice is made.
const ConfirmationDialog = ({ finding, submitting = false, error = '', onSubmit, onClose }) => {
  const confirmation = finding?.confirmation || {};
  const options = Array.isArray(confirmation.options) ? confirmation.options : [];
  const unsure = confirmation.unsure_option;
  const allOptions = unsure ? [...options, unsure] : options;
  const [selected, setSelected] = useState('');

  if (!confirmation.eligible || !allOptions.length) return null;

  const excerpt = finding?.evidence?.excerpt;
  const page = finding?.evidence?.page ?? finding?.page;

  const handleConfirm = () => {
    if (!selected || submitting) return;
    onSubmit?.(finding.finding_id, selected);
  };

  return (
    <div
      data-testid="cv2-confirmation-dialog"
      className="rounded-xl border border-amber-400/30 bg-zinc-950/90 p-4 sm:p-5"
    >
      <div className="flex items-start justify-between gap-3">
        <h4 className="text-sm font-semibold text-zinc-100">{finding?.title || 'Conferma necessaria'}</h4>
        <button
          type="button"
          aria-label="Chiudi"
          data-testid="cv2-confirmation-close"
          onClick={onClose}
          className="text-zinc-500 hover:text-zinc-300"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <p className="mt-2 text-sm text-zinc-200">{confirmation.question}</p>

      {excerpt && (
        <figure className="mt-3 rounded-lg border border-zinc-800 bg-zinc-900/60 p-3">
          <div className="flex items-start gap-2">
            <Quote className="mt-0.5 h-3.5 w-3.5 shrink-0 text-zinc-500" />
            <blockquote className="text-xs italic leading-5 text-zinc-300">{excerpt}</blockquote>
          </div>
          {page ? <figcaption className="mt-1.5 text-xs text-gold">p. {page}</figcaption> : null}
        </figure>
      )}

      <fieldset className="mt-3 space-y-2" data-testid="cv2-confirmation-options">
        {allOptions.map((opt) => (
          <label
            key={opt.option_id}
            className={`flex cursor-pointer items-center gap-2.5 rounded-lg border p-2.5 text-sm ${
              selected === opt.option_id
                ? 'border-gold/50 bg-gold/5 text-zinc-100'
                : 'border-zinc-800 bg-zinc-950 text-zinc-300 hover:border-zinc-700'
            }`}
          >
            <input
              type="radio"
              name={`conf-${finding.finding_id}`}
              value={opt.option_id}
              checked={selected === opt.option_id}
              onChange={() => setSelected(opt.option_id)}
              className="accent-gold"
            />
            <span>{opt.label}</span>
          </label>
        ))}
      </fieldset>

      {error ? (
        <p data-testid="cv2-confirmation-error" className="mt-2 text-xs text-red-300">{error}</p>
      ) : null}

      <div className="mt-4 flex items-center justify-end gap-2">
        <Button type="button" variant="ghost" size="sm" onClick={onClose} disabled={submitting}>
          Annulla
        </Button>
        <Button
          type="button"
          size="sm"
          data-testid="cv2-confirmation-submit"
          onClick={handleConfirm}
          disabled={!selected || submitting}
        >
          {submitting ? <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : null}
          Conferma
        </Button>
      </div>
    </div>
  );
};

export default ConfirmationDialog;
export { ConfirmationDialog };
