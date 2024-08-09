// src/components/BulkOperations.tsx
import React, { useEffect, useState } from 'react';
import modalConfig from './modalConfig';
import { downloadCSV } from './downloadUtils';
import { getUserIDFromCookies } from './crudUtils';
import Papa from 'papaparse';

const BulkOperationsForm: React.FC = () => {
  const apiHost = process.env.NEXT_PUBLIC_API_HOST;
  //const userId = getUserIDFromCookies();
  const [userId, setUserId] = useState<string | undefined>(undefined);
  const [operation, setOperation] = useState<string>('');
  const [modal, setModal] = useState<string>('');
  const [timeLimit, setTimeLimit] = useState<string>('');
  const [file, setFile] = useState<File | null>(null);

  useEffect(() => {
    setUserId(getUserIDFromCookies());
  }, []);


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

    const missingFields: string[] = [];
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

        downloadCSV(data, columns, 'bulk_read');
      } catch (error) {
        console.error('There was an error!', error);
      }
    } else if (operation === 'create') {
      if (file) {
        try {
          const text = await file.text();
          const parsedData = Papa.parse(text, { header: true });

          if (parsedData.errors.length > 0) {
            console.error('Error parsing CSV file:', parsedData.errors);
            alert('Error parsing CSV File');
            return;
          }

          if (!parsedData.data || parsedData.data.length === 0) {
            alert('No data found in the CSV file');
            return;
          }

          const firstRow = parsedData.data[0];
          if (!firstRow) {
            alert('The file appears to be empty or improperly formatted.');
            return;
          }

          const columns = Object.keys(firstRow);
          const bulkValues = parsedData.data.map((row: any) =>
            columns.map(column => row[column])
          );

          const payload = {
            user_id: userId, // you have to replace this with actual user_id
            columns,
            data: bulkValues,
          };

          const response = await fetch(`${apiHost}bulk_create/${modal}`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload),
          });

          const result = await response.json();

          if (response.ok) {
            alert('Data uploaded successfully');
          } else {
            alert(`Error: ${result.error}`);
          }
        } catch (error) {
          console.error('There was an error uploading the data!', error);
        }
      } else {
        alert('Please upload a file.');
      }
    } else {
      alert('Form submitted successfully.');
    }
  };

  const handleDownloadTemplate = () => {
    if (modal && operation === 'create') {
      const columns = modalConfig[modal].scopes.read.filter(
        column => column !== 'id' && column !== 'user_id' && column !== 'created_at' && column !== 'updated_at'
      );
      downloadCSV([], columns, `${modal}_create_template`);
    }

    if (modal && operation === 'update') {
      const columns = modalConfig[modal].scopes.read.filter(
        column => column !== 'user_id' && column !== 'created_at' && column !== 'updated_at'
      );
      downloadCSV([], columns, `${modal}_update_template`);
    }

    if (modal && operation === 'delete') {
      const columns = modalConfig[modal].scopes.read.filter(
        column => column !== 'user_id' && column !== 'created_at' && column !== 'updated_at'
      );
      downloadCSV([], columns, `${modal}_delete_template`);
    }
  };

  return (
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

      {(operation === 'create' || operation === 'update' || operation === 'delete') && modal && (
        <div className="mb-4">
          <button
            type="button"
            onClick={handleDownloadTemplate}
            className="bg-black text-yellow-100/50 border border-yellow-100/30 hover:bg-yellow-100 hover:text-black py-2 px-6 rounded-lg"
          >
            Download Template
          </button>
        </div>
      )}
      {(operation === 'create' || operation === 'update' || operation === 'delete') && (
        <div className="mb-4">
          <label htmlFor="file" className="block text-sm text-yellow-100/50 mb-2">Upload CSV</label>
          <input
            type="file"
            id="file"
            onChange={(e) => setFile(e.target.files ? e.target.files[0] : null)}
            className="block w-full text-yellow-100/50 bg-black border border-yellow-100/30 rounded-lg p-2.5"
          />
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
  );
};

export default BulkOperationsForm;


