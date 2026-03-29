import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { useParams, useNavigate } from 'react-router-dom';
import AdminLayout from './AdminLayout';
import { Button } from '../../components/ui/button';
import { Loader2 } from 'lucide-react';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;
const PAGE_SIZE = 10;

const CREDIT_LABELS = {
  perizia_scans_remaining: 'Crediti perizia',
  image_scans_remaining: 'Crediti immagini',
  assistant_messages_remaining: 'Messaggi assistente',
};

const ENTRY_TYPE_LABELS = {
  opening_balance: 'Saldo iniziale',
  admin_adjustment: 'Variazione admin',
  plan_purchase: 'Acquisto piano',
  perizia_upload: 'Analisi perizia',
  image_forensics: 'Analisi immagini',
  assistant_message: 'Messaggio assistente',
  top_up: 'Ricarica',
  subscription_reset: 'Reset abbonamento',
  system_correction: 'Correzione sistema',
};

const formatDateTime = (value) => {
  if (!value) return '-';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '-';
  return parsed.toLocaleString('it-IT');
};

const formatCurrency = (value, currency = 'EUR') => new Intl.NumberFormat('it-IT', {
  style: 'currency',
  currency: String(currency || 'EUR').toUpperCase(),
  minimumFractionDigits: 2,
}).format(Number(value || 0));

const SectionShell = ({ title, subtitle, children, action }) => (
  <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
    <div className="flex items-start justify-between gap-4 mb-4">
      <div>
        <h3 className="text-lg font-serif font-bold text-zinc-100">{title}</h3>
        {subtitle && <p className="mt-1 text-sm text-zinc-500">{subtitle}</p>}
      </div>
      {action}
    </div>
    {children}
  </div>
);

const DataState = ({ loading, error, emptyLabel, children }) => {
  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-zinc-400">
        <Loader2 className="h-4 w-4 animate-spin" />
        Caricamento...
      </div>
    );
  }
  if (error) {
    return <p className="text-sm text-amber-300">{error}</p>;
  }
  if (emptyLabel) {
    return <p className="text-sm text-zinc-500">{emptyLabel}</p>;
  }
  return children;
};

const AdminUserDetail = () => {
  const { user_id } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [ledgerEntries, setLedgerEntries] = useState([]);
  const [ledgerTotal, setLedgerTotal] = useState(0);
  const [ledgerLoading, setLedgerLoading] = useState(true);
  const [ledgerLoadingMore, setLedgerLoadingMore] = useState(false);
  const [ledgerError, setLedgerError] = useState('');
  const [billingRecords, setBillingRecords] = useState([]);
  const [billingTotal, setBillingTotal] = useState(0);
  const [billingLoading, setBillingLoading] = useState(true);
  const [billingLoadingMore, setBillingLoadingMore] = useState(false);
  const [billingError, setBillingError] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    const fetchDetail = async () => {
      setLoading(true);
      try {
        const response = await axios.get(`${API_URL}/api/admin/users/${user_id}`, { withCredentials: true });
        setData(response.data);
      } catch (error) {
        toast.error('Errore nel caricamento utente');
      } finally {
        setLoading(false);
      }
    };

    fetchDetail();
  }, [user_id]);

  useEffect(() => {
    const fetchLedger = async (reset = true) => {
      const nextSkip = reset ? 0 : ledgerEntries.length;
      if (reset) {
        setLedgerLoading(true);
        setLedgerError('');
      } else {
        setLedgerLoadingMore(true);
      }

      try {
        const response = await axios.get(`${API_URL}/api/admin/users/${user_id}/ledger`, {
          params: { limit: PAGE_SIZE, skip: nextSkip },
          withCredentials: true,
        });
        const nextEntries = response.data?.entries || [];
        setLedgerEntries((current) => (reset ? nextEntries : [...current, ...nextEntries]));
        setLedgerTotal(response.data?.total || 0);
      } catch (error) {
        setLedgerError('Impossibile caricare i movimenti crediti.');
      } finally {
        if (reset) {
          setLedgerLoading(false);
        } else {
          setLedgerLoadingMore(false);
        }
      }
    };

    fetchLedger(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user_id]);

  useEffect(() => {
    const fetchBillingRecords = async (reset = true) => {
      const nextSkip = reset ? 0 : billingRecords.length;
      if (reset) {
        setBillingLoading(true);
        setBillingError('');
      } else {
        setBillingLoadingMore(true);
      }

      try {
        const response = await axios.get(`${API_URL}/api/admin/users/${user_id}/billing-records`, {
          params: { limit: PAGE_SIZE, skip: nextSkip },
          withCredentials: true,
        });
        const nextRecords = response.data?.records || [];
        setBillingRecords((current) => (reset ? nextRecords : [...current, ...nextRecords]));
        setBillingTotal(response.data?.total || 0);
      } catch (error) {
        setBillingError('Impossibile caricare i record di fatturazione.');
      } finally {
        if (reset) {
          setBillingLoading(false);
        } else {
          setBillingLoadingMore(false);
        }
      }
    };

    fetchBillingRecords(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user_id]);

  const user = data?.user || {};
  const ledgerEmpty = !ledgerLoading && !ledgerError && ledgerEntries.length === 0 ? 'Nessun movimento crediti registrato.' : '';
  const billingEmpty = !billingLoading && !billingError && billingRecords.length === 0 ? 'Nessun record di fatturazione registrato.' : '';

  return (
    <AdminLayout title="Dettaglio Utente" subtitle={user.email || ''}>
      <div className="mb-6">
        <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => navigate('/admin/users')}>
          Back to Users
        </Button>
      </div>

      {loading ? (
        <div className="text-zinc-400 font-mono text-sm">Loading...</div>
      ) : (
        <>
          <div className="mb-8 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-1">Current account state</p>
              <p className="text-2xl font-serif font-bold text-zinc-100 capitalize">{user.plan || '-'}</p>
              <p className="text-xs text-zinc-500 mt-2">Created {user.created_at ? new Date(user.created_at).toLocaleDateString() : '-'}</p>
              <p className="text-xs text-zinc-500 mt-1">Last active {formatDateTime(user.last_active_at)}</p>
            </div>
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-2">Quota state</p>
              <p className="text-sm text-zinc-200">Perizie: {user.quota?.perizia_scans_remaining || 0}</p>
              <p className="text-sm text-zinc-200">Immagini: {user.quota?.image_scans_remaining || 0}</p>
              <p className="text-sm text-zinc-200">Assistente: {user.quota?.assistant_messages_remaining || 0}</p>
            </div>
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-2">Billing summary</p>
              <p className="text-sm text-zinc-200">Latest status: {user.financial_summary?.latest_billing_status || '-'}</p>
              <p className="text-sm text-zinc-200">Records: {user.financial_summary?.billing_records_count || 0}</p>
              <p className="text-sm text-zinc-200">Purchase type: {user.financial_summary?.latest_purchase_type || '-'}</p>
              <p className="text-xs text-zinc-500 mt-2">Latest credit movement {formatDateTime(user.financial_summary?.latest_credit_movement_at)}</p>
            </div>
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-1">Note interne</p>
              <p className="text-sm text-zinc-200">{user.notes?.internal_status || 'N/A'}</p>
              <p className="text-xs text-zinc-400 mt-2">{user.notes?.note || 'Nessuna nota'}</p>
              <p className="text-xs text-zinc-500 mt-2">Tags: {(user.notes?.tags || []).join(', ') || '-'}</p>
            </div>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-8">
            <SectionShell
              title="Movimenti crediti"
              subtitle={ledgerTotal ? `${ledgerEntries.length} di ${ledgerTotal} movimenti` : 'Storico ledger utente'}
              action={!ledgerLoading && !ledgerError && ledgerEntries.length < ledgerTotal ? (
                <Button
                  variant="outline"
                  className="border-zinc-700 text-zinc-300"
                  disabled={ledgerLoadingMore}
                  onClick={async () => {
                    setLedgerLoadingMore(true);
                    try {
                      const response = await axios.get(`${API_URL}/api/admin/users/${user_id}/ledger`, {
                        params: { limit: PAGE_SIZE, skip: ledgerEntries.length },
                        withCredentials: true,
                      });
                      setLedgerEntries((current) => [...current, ...(response.data?.entries || [])]);
                      setLedgerTotal(response.data?.total || 0);
                    } catch (error) {
                      setLedgerError('Impossibile caricare altri movimenti crediti.');
                    } finally {
                      setLedgerLoadingMore(false);
                    }
                  }}
                >
                  {ledgerLoadingMore ? 'Caricamento...' : 'Carica altri'}
                </Button>
              ) : null}
            >
              <DataState loading={ledgerLoading} error={ledgerError} emptyLabel={ledgerEmpty}>
                <div className="space-y-3">
                  {ledgerEntries.map((entry) => (
                    <div key={entry.ledger_id} className="rounded-xl border border-zinc-800 bg-zinc-950 p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="text-sm font-semibold text-zinc-100">{entry.description_it || 'Movimento crediti'}</p>
                          <p className="mt-1 text-xs text-zinc-500">{formatDateTime(entry.created_at)}</p>
                        </div>
                        <span className={`rounded-full border px-2 py-1 text-[11px] uppercase tracking-wide ${entry.direction === 'credit' ? 'border-emerald-500/20 text-emerald-300' : 'border-amber-500/20 text-amber-300'}`}>
                          {entry.direction === 'credit' ? '+' : '-'}{entry.amount || 0}
                        </span>
                      </div>
                      <div className="mt-3 grid grid-cols-1 gap-3 text-xs text-zinc-400 sm:grid-cols-2">
                        <div>{CREDIT_LABELS[entry.quota_field] || entry.quota_field || '-'}</div>
                        <div>{ENTRY_TYPE_LABELS[entry.entry_type] || entry.entry_type || '-'}</div>
                        <div>Saldo prima: {entry.balance_before || 0}</div>
                        <div>Saldo dopo: {entry.balance_after || 0}</div>
                        {typeof entry.metadata?.pages_count === 'number' && entry.metadata.pages_count > 0 && (
                          <div>{entry.metadata.pages_count} pagine</div>
                        )}
                        {entry.reference_id && entry.reference_id !== 'n/a' && (
                          <div className="font-mono text-[11px] text-zinc-500">Rif. {entry.reference_id}</div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </DataState>
            </SectionShell>

            <SectionShell
              title="Record di fatturazione"
              subtitle={billingTotal ? `${billingRecords.length} di ${billingTotal} record` : 'Storico billing utente'}
              action={!billingLoading && !billingError && billingRecords.length < billingTotal ? (
                <Button
                  variant="outline"
                  className="border-zinc-700 text-zinc-300"
                  disabled={billingLoadingMore}
                  onClick={async () => {
                    setBillingLoadingMore(true);
                    try {
                      const response = await axios.get(`${API_URL}/api/admin/users/${user_id}/billing-records`, {
                        params: { limit: PAGE_SIZE, skip: billingRecords.length },
                        withCredentials: true,
                      });
                      setBillingRecords((current) => [...current, ...(response.data?.records || [])]);
                      setBillingTotal(response.data?.total || 0);
                    } catch (error) {
                      setBillingError('Impossibile caricare altri record di fatturazione.');
                    } finally {
                      setBillingLoadingMore(false);
                    }
                  }}
                >
                  {billingLoadingMore ? 'Caricamento...' : 'Carica altri'}
                </Button>
              ) : null}
            >
              <DataState loading={billingLoading} error={billingError} emptyLabel={billingEmpty}>
                <div className="space-y-3">
                  {billingRecords.map((record) => (
                    <div key={record.billing_record_id} className="rounded-xl border border-zinc-800 bg-zinc-950 p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="text-sm font-semibold text-zinc-100">{record.description_it || 'Billing record'}</p>
                          <p className="mt-1 text-xs text-zinc-500">{formatDateTime(record.created_at)}</p>
                        </div>
                        <span className="rounded-full border border-zinc-700 px-2 py-1 text-[11px] uppercase tracking-wide text-zinc-300">
                          {record.status || '-'}
                        </span>
                      </div>
                      <div className="mt-3 grid grid-cols-1 gap-3 text-xs text-zinc-400 sm:grid-cols-2">
                        <div>{formatCurrency(record.amount_total, record.currency)}</div>
                        <div>{record.purchase_type || '-'}</div>
                        <div>Piano: {record.plan_id || '-'}</div>
                        <div>Provider: {record.payment_provider || '-'}</div>
                        <div>Invoice: {record.invoice_status || '-'}</div>
                        <div>Paid: {formatDateTime(record.paid_at)}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </DataState>
            </SectionShell>
          </div>

          <SectionShell title="Recent Activity" subtitle="Ultime analisi e sessioni utente">
            <div className="space-y-3">
              {(data?.recent_activity || []).map((item) => (
                <div key={`${item.type}-${item.id}`} className="flex flex-col gap-3 rounded-lg border border-zinc-800 bg-zinc-950 p-3 sm:flex-row sm:items-center sm:justify-between">
                  <div className="min-w-0">
                    <p className="text-sm text-zinc-200 uppercase font-mono">{item.type}</p>
                    <p className="text-xs text-zinc-500">Case: {item.case_id || '-'} | Run: {item.run_id || '-'}</p>
                    <p className="text-xs text-zinc-500">Created: {item.created_at ? new Date(item.created_at).toLocaleString() : '-'}</p>
                  </div>
                  <div className="text-xs text-zinc-400 sm:text-right">
                    {item.summary?.semaforo && <div>Semaforo: {item.summary.semaforo}</div>}
                    {item.summary?.image_count !== undefined && <div>Images: {item.summary.image_count}</div>}
                    {item.summary?.question_preview && <div>{item.summary.question_preview}</div>}
                  </div>
                </div>
              ))}
              {(!data?.recent_activity || data.recent_activity.length === 0) && (
                <div className="text-zinc-500 text-sm">Nessuna attività recente</div>
              )}
            </div>
          </SectionShell>
        </>
      )}
    </AdminLayout>
  );
};

export default AdminUserDetail;
