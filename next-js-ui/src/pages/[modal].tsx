import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import DynamicTable from '../components/DynamicTable';
import Sidebar from '../components/Sidebar';
import '../app/globals.css';

const ModalPage: React.FC = () => {
  const router = useRouter();
  const { modal } = router.query;


  return (
    <div className="flex">
      <Sidebar />
      <div className="bg-black min-h-screen flex-1 p-8 overflow-x-auto">
        <div className="container mx-auto">
          <h1 className="text-yellow-100/50 mr-2 text-right">
            {(modal as string)} table
          </h1>
            <DynamicTable
              modal={modal as string}
            />
        </div>
      </div>
    </div>
  );
};

export default ModalPage;

