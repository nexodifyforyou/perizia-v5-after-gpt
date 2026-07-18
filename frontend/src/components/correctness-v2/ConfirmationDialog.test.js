import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import { ConfirmationDialog } from './ConfirmationDialog';

function mount(ui) {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  act(() => { root.render(ui); });
  return { container, unmount: () => act(() => { root.unmount(); }) };
}

const q = (c, sel) => c.querySelector(sel);
const qa = (c, sel) => Array.from(c.querySelectorAll(sel));

const finding = {
  finding_id: 'occ-1',
  title: 'Stato di occupazione',
  page: 3,
  evidence: { page: 3, excerpt: 'immobile occupato dal conduttore' },
  confirmation: {
    eligible: true,
    question: "Secondo la pagina 3, l'immobile risulta:",
    options: [
      { option_id: 'occupato_opponibile', label: 'Occupato con contratto opponibile' },
      { option_id: 'libero', label: "L'immobile è libero" },
    ],
    unsure_option: { option_id: 'non_sicuro', label: 'Non sono sicuro' },
  },
};

// 15. renders question + page + verbatim excerpt
test('renders question, page and verbatim excerpt', () => {
  const { container, unmount } = mount(<ConfirmationDialog finding={finding} onSubmit={() => {}} onClose={() => {}} />);
  const dlg = q(container, '[data-testid="cv2-confirmation-dialog"]');
  expect(dlg.textContent).toContain("Secondo la pagina 3, l'immobile risulta:");
  expect(dlg.textContent).toContain('immobile occupato dal conduttore');
  expect(dlg.textContent).toContain('p. 3');
  unmount();
});

// 16. options 2-4 including "Non sono sicuro"
test('offers the backend options plus Non sono sicuro', () => {
  const { container, unmount } = mount(<ConfirmationDialog finding={finding} onSubmit={() => {}} onClose={() => {}} />);
  const radios = qa(container, '[data-testid="cv2-confirmation-options"] input[type="radio"]');
  expect(radios.length).toBe(3);
  expect(container.textContent).toContain('Non sono sicuro');
  unmount();
});

// 17. confirm disabled until a selection is made
test('confirm is disabled until a choice is selected', () => {
  const { container, unmount } = mount(<ConfirmationDialog finding={finding} onSubmit={() => {}} onClose={() => {}} />);
  const submit = q(container, '[data-testid="cv2-confirmation-submit"]');
  expect(submit.disabled).toBe(true);
  act(() => { qa(container, 'input[type="radio"]')[1].click(); });
  expect(q(container, '[data-testid="cv2-confirmation-submit"]').disabled).toBe(false);
  unmount();
});

// 18. submit calls handler with the chosen option_id
test('submit calls the handler with the finding id and option id', () => {
  const onSubmit = jest.fn();
  const { container, unmount } = mount(<ConfirmationDialog finding={finding} onSubmit={onSubmit} onClose={() => {}} />);
  act(() => { qa(container, 'input[type="radio"]')[0].click(); });
  act(() => { q(container, '[data-testid="cv2-confirmation-submit"]').click(); });
  expect(onSubmit).toHaveBeenCalledWith('occ-1', 'occupato_opponibile');
  unmount();
});

// 19. error state renders the message
test('renders an error message', () => {
  const { container, unmount } = mount(
    <ConfirmationDialog finding={finding} onSubmit={() => {}} onClose={() => {}} error="Impossibile salvare." />
  );
  expect(q(container, '[data-testid="cv2-confirmation-error"]').textContent).toContain('Impossibile salvare.');
  unmount();
});

// 20. cancel closes without submitting
test('cancel closes without submitting', () => {
  const onSubmit = jest.fn();
  const onClose = jest.fn();
  const { container, unmount } = mount(<ConfirmationDialog finding={finding} onSubmit={onSubmit} onClose={onClose} />);
  // the close (X) button
  act(() => { q(container, '[data-testid="cv2-confirmation-close"]').click(); });
  expect(onClose).toHaveBeenCalled();
  expect(onSubmit).not.toHaveBeenCalled();
  unmount();
});
