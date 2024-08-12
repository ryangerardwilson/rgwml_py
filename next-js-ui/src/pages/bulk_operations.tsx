// pages/bulk_operations.tsx
import React from 'react';
import Sidebar from '../components/Sidebar';
import BulkOperationsForm from '../components/BulkOperations';
import '../app/globals.css';

const BulkOperationsPage: React.FC = () => {
  return (
    <div className="flex">
      <Sidebar />
      <div className="bg-black min-h-screen flex-1 p-8 overflow-x-auto">
        <div className="container mx-auto">
          <h1 className="text-yellow-100/50 mr-2 text-right">bulk_operations</h1>
          <div className="bg-black border border-yellow-100/30 rounded-lg text-yellow-100 p-4 text-sm flex justify-center">
            <div className="w-96 max-w-xl">
              <BulkOperationsForm />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BulkOperationsPage;