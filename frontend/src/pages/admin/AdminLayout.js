import React from 'react';
import { useAuth } from '../../context/AuthContext';
import { Sidebar } from '../Dashboard';

const AdminLayout = ({ title, subtitle, children }) => {
  const { user, logout } = useAuth();

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      <main className="ml-64 p-8">
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">{title}</h1>
          {subtitle && <p className="text-zinc-400">{subtitle}</p>}
        </div>
        {children}
      </main>
    </div>
  );
};

export default AdminLayout;
