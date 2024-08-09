// src/pages/bulk_operations.tsx
import React, { useState } from 'react';
import Sidebar from '../components/Sidebar';
import modalConfig from '../components/modalConfig';
import '../app/globals.css';
import { downloadCSV } from '../components/downloadUtils'; // Import the downloadCSV function

const BulkOperationsPage: React.FC = () => {
  const apiHost = process.env.NEXT_PUBLIC_API_HOST;
  const [operation, setOperation] = useState<string>('');
  const [modal, setModal] = useState<string>('');
  const [timeLimit, setTimeLimit] = useState<string>('');

  const modalNames = Object.keys(modalConfig);
  const timeLimits = [
    'today',
    'since_yesterday',
    'since_last_7_days',
    'since_last_14_days',
    'since_last_28_days',
    'since_last_90_days',
  ];

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const missingFields = [];
    if (operation === '') {
      missingFields.push('operation');
    }
    if (modal === '') {
      missingFields.push('modal');
    }
    if (operation === 'read' && timeLimit === '') {
      missingFields.push('time_limit');
    }

    if (missingFields.length > 0) {
      alert(`Please fill all required fields: ${missingFields.join(', ')}`);
      return;
    }

    if (operation === 'read') {
      try {
        const response = await fetch(`${apiHost}bulk_read/${modal}/${timeLimit}`);
        const responseData = await response.json();
        const { columns, data } = responseData;

        // Use the downloadCSV function
        downloadCSV(data, columns, 'bulk_read');
      } catch (error) {
        console.error('There was an error!', error);
      }
    } else {
      alert('Form submitted successfully.');
    }
  };

  return (
    <div className="flex">
      <Sidebar />
      <div className="bg-black min-h-screen flex-1 p-8 overflow-x-auto">
        <div className="container mx-auto">
          <h1 className="text-yellow-100/50 mr-2 text-right">bulk_operations</h1>
          <div className="bg-black border border-yellow-100/30 rounded-lg text-yellow-100 p-4 text-sm flex justify-center">
            <div className="w-96 max-w-xl">
              <form onSubmit={handleSubmit}>
                <div className="mb-4">
                  <label htmlFor="operation" className="block text-sm text-yellow-100/50 mb-2">operation</label>
                  <select
                    id="operation"
                    value={operation}
                    onChange={(e) => setOperation(e.target.value)}
                    className="block w-full p-2.5 bg-black border border-yellow-100/30 rounded-lg text-yellow-100/50"
                  >
                    <option value="" disabled></option>
                    <option value="create">create</option>
                    <option value="read">read</option>
                    <option value="update">update</option>
                    <option value="delete">delete</option>
                  </select>
                </div>

                <div className="mb-4">
                  <label htmlFor="modal" className="block text-sm text-yellow-100/50 mb-2">modal</label>
                  <select
                    id="modal"
                    value={modal}
                    onChange={(e) => setModal(e.target.value)}
                    className="block w-full p-2.5 bg-black border border-yellow-100/30 rounded-lg text-yellow-100/50"
                  >
                    <option value="" disabled></option>
                    {modalNames.map((modalName) => (
                      <option key={modalName} value={modalName}>{modalName}</option>
                    ))}
                  </select>
                </div>

                {operation === 'read' && (
                  <div className="mb-4">
                    <label htmlFor="timeLimit" className="block text-sm text-yellow-100/50 mb-2">time_limit</label>
                    <select
                      id="timeLimit"
                      value={timeLimit}
                      onChange={(e) => setTimeLimit(e.target.value)}
                      className="block w-full p-2.5 bg-black border border-yellow-100/30 rounded-lg text-yellow-100/50"
                    >
                      <option value="" disabled>Select a time limit</option>
                      {timeLimits.map((limit) => (
                        <option key={limit} value={limit}>{limit}</option>
                      ))}
                    </select>
                  </div>
                )}

                <div className="text-right">
                  <button
                    type="submit"
                    className="bg-yellow-100 text-black py-2.5 px-6 rounded-lg mt-4"
                  >
                    Submit
                  </button>
                </div>

              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BulkOperationsPage;

