import React from 'react';
import { Shield } from 'lucide-react';
import { Button } from './ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from './ui/dialog';

const PreLoginNoticeDialog = ({ open, onOpenChange, onConfirm }) => {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md border border-gold/20 bg-zinc-950 p-0 text-zinc-100 shadow-[0_24px_80px_rgba(0,0,0,0.55)]">
        <div className="rounded-lg border border-white/5 bg-gradient-to-b from-zinc-900 via-zinc-950 to-black p-6">
          <DialogHeader className="space-y-3 text-left">
            <div className="flex h-11 w-11 items-center justify-center rounded-xl border border-gold/20 bg-gold/10 text-gold">
              <Shield className="h-5 w-5" />
            </div>
            <DialogTitle className="font-serif text-2xl text-zinc-50">
              Accesso sicuro
            </DialogTitle>
            <DialogDescription className="text-sm leading-6 text-zinc-300">
              Per completare l’accesso, verrai reindirizzato a un partner tecnologico che gestisce l’autenticazione Google in modo sicuro. Al termine, tornerai automaticamente su PeriziaScan.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 rounded-xl border border-zinc-800 bg-zinc-900/80 px-4 py-3">
            <p className="text-xs leading-5 text-zinc-400">
              Durante il login potresti vedere il nome del provider di autenticazione esterno.
            </p>
          </div>

          <DialogFooter className="mt-6 flex-col-reverse gap-3 sm:flex-row sm:justify-end sm:space-x-0">
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              className="border-zinc-700 bg-transparent text-zinc-300 hover:bg-zinc-900 hover:text-zinc-100"
            >
              Annulla
            </Button>
            <Button
              type="button"
              onClick={onConfirm}
              className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold gold-glow"
            >
              Continua con Google
            </Button>
          </DialogFooter>
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default PreLoginNoticeDialog;
