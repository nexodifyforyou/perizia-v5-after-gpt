import { downloadPdfBlob } from './pdfDownload';

describe('downloadPdfBlob', () => {
  const originalCreateObjectURL = window.URL.createObjectURL;
  const originalRevokeObjectURL = window.URL.revokeObjectURL;
  const originalCreateElement = document.createElement.bind(document);

  afterEach(() => {
    jest.restoreAllMocks();
    window.URL.createObjectURL = originalCreateObjectURL;
    window.URL.revokeObjectURL = originalRevokeObjectURL;
    document.createElement = originalCreateElement;
  });

  test('creates a PDF blob and triggers download with .pdf extension', () => {
    const click = jest.fn();
    const remove = jest.fn();
    const setAttribute = jest.fn();

    const anchor = {
      href: '',
      setAttribute,
      click,
      remove,
    };

    jest.spyOn(document.body, 'appendChild').mockImplementation(() => anchor);
    document.createElement = jest.fn((tagName) => {
      if (tagName === 'a') return anchor;
      return originalCreateElement(tagName);
    });

    window.URL.createObjectURL = jest.fn(() => 'blob:mock');
    window.URL.revokeObjectURL = jest.fn();

    const raw = new Blob(['%PDF-1.7 test'], { type: 'application/octet-stream' });
    downloadPdfBlob(raw, 'analysis_abc123');

    expect(window.URL.createObjectURL).toHaveBeenCalledTimes(1);
    const blobArg = window.URL.createObjectURL.mock.calls[0][0];
    expect(blobArg).toBeInstanceOf(Blob);
    expect(blobArg.type).toBe('application/pdf');
    expect(setAttribute).toHaveBeenCalledWith('download', 'nexodify_report_analysis_abc123.pdf');
    expect(click).toHaveBeenCalledTimes(1);
    expect(remove).toHaveBeenCalledTimes(1);
    expect(window.URL.revokeObjectURL).toHaveBeenCalledWith('blob:mock');
  });
});
