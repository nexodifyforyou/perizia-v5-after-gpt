export const downloadPdfBlob = (blobData, analysisId) => {
  const sourceBlob = blobData instanceof Blob ? blobData : new Blob([blobData], { type: 'application/pdf' });
  const pdfBlob =
    sourceBlob.type === 'application/pdf'
      ? sourceBlob
      : new Blob([sourceBlob], { type: 'application/pdf' });

  const url = window.URL.createObjectURL(pdfBlob);
  const link = document.createElement('a');
  link.href = url;
  link.setAttribute('download', `nexodify_report_${analysisId}.pdf`);
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
};
