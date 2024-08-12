import React, { useEffect, useState } from 'react';
import modalConfig from './modalConfig';
import { downloadCSV } from './downloadUtils';
import { getUserIDFromCookies } from './crudUtils';
import { handleReadOperation, handleCreateOperation, handleUpdateOperation, handleDeleteOperation } from './bulkOperationsUtils';
import Papa from 'papaparse';

const BulkOperationsForm: React.FC = () => {
  const apiHost = process.env.NEXT_PUBLIC_API_HOST;

  const [userId, setUserId] = useState<string | undefined>(undefined);
  const [operation, setOperation] = useState<string>('');
  const [modal, setModal] = useState<string>('');
  const [timeLimit, setTimeLimit] = useState<string>('');
  const [file, setFile] = useState<File | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [isColumnSelectionVisible, setIsColumnSelectionVisible] = useState<boolean>(false);
  const [isValidFile, setIsValidFile] = useState<boolean>(true);  // State to manage file validity


  useEffect(() => {
    setUserId(getUserIDFromCookies());
  }, []);

  useEffect(() => {
    const parseFile = async () => {
      if (file) {
        const text = await file.text();
        const parsedData = Papa.parse(text, { header: true });
        if (parsedData.errors.length > 0) {
          console.error('Error parsing CSV file:', parsedData.errors);
          alert('Error parsing CSV File');
          setIsValidFile(false);
          return;
        }
        if (!parsedData.data || parsedData.data.length === 0) {
          alert('No data found in the CSV file');
          setIsValidFile(false);
          return;
        }
        const firstRow = parsedData.data[0];
        if (!firstRow) {
          alert('The file appears to be empty or improperly formatted.');
          setIsValidFile(false);
          return;
        }

        const cols = Object.keys(firstRow).filter(col => !['user_id', 'created_at', 'updated_at'].includes(col));

        // Get the read scope columns for the selected modal
        const readScopeColumns = modalConfig[modal]?.scopes?.read || [];

        console.log(readScopeColumns);
        console.log(cols);

        // Check if every column in the file is in the read scope
        const isValidFile = cols.every(col => readScopeColumns.includes(col));

        if (!isValidFile) {
          setIsValidFile(false);
          return;
        }

        setColumns(cols);
        setIsColumnSelectionVisible(true);
        setIsValidFile(true);
      }
    };

    if (operation === 'update' && file) {
      parseFile();
    }
  }, [file, operation, modal]);

  const modalNames = Object.keys(modalConfig);
  const timeLimits = [
    'today',
    'since_yesterday',
    'since_last_7_days',
    'since_last_14_days',
    'since_last_28_days',
    'since_last_90_days',
  ];

  const validateForm = (): string[] => {
    const missingFields: string[] = [];
    if (operation === '') missingFields.push('operation');
    if (modal === '') missingFields.push('modal');
    if (operation === 'read' && timeLimit === '') missingFields.push('time_limit');
    return missingFields;
  };

const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
  event.preventDefault();
  
  const missingFields = validateForm();
  if (missingFields.length > 0) {
    alert(`Please fill all required fields: ${missingFields.join(', ')}`);
    return;
  }

  if (!isValidFile) {
    alert('The file uploaded does not relate to the modal selected.');
    return;
  }

  // Ensure the apiHost, modal, and timeLimit are non-null, non-undefined strings
  if (!apiHost || !modal) {
    alert('API host and modal must be selected.');
    return;
  }
  
  try {
    if (operation === 'read') {
      await handleReadOperation(apiHost, modal, timeLimit || '');
    } else if (operation === 'create') {
      if (file) {
        await handleCreateOperation(apiHost, modal, file, userId);
        alert('Data created successfully.');
        window.location.reload();
      } else {
        alert('Please upload a file.');
      }
    } else if (operation === 'update') {
      if (isColumnSelectionVisible && selectedColumns.length > 0) {
        // Proceed with the update operation
        await handleUpdateOperation(apiHost, modal, file, userId, selectedColumns);
        alert('Data updated successfully.');
        window.location.reload();
      } else {
        alert('Please select columns to update.');
      }
    } else if (operation === 'delete') {
      if (file) {
        // Parse the file to extract ids
        const text = await file.text();
        const parsedData = Papa.parse(text, { header: true });
        if (parsedData.errors.length > 0) {
          console.error('Error parsing CSV file:', parsedData.errors);
          alert('Error parsing CSV file');
          return;
        }
        if (!parsedData.data || parsedData.data.length === 0) {
          alert('No data found in the CSV file');
          return;
        }
        const ids = parsedData.data.map((row: any) => parseInt(row['id'], 10)).filter((id: number) => !isNaN(id));
        if (ids.length === 0) {
          alert('No valid IDs found in the CSV file');
          return;
        }

        await handleDeleteOperation(apiHost, modal, userId || '', ids);
        alert('Data deleted successfully.');
        window.location.reload();
      } else {
        alert('Please upload a file.');
      }
    } else {
      alert('Form submitted successfully.');
    }
  } catch (error) {
    console.error('There was an error handling the operation!', error);
    alert('There was an error handling the operation.');
  }
};


  const handleDownloadTemplate = () => {
    const columns = modalConfig[modal].scopes.read.filter(
      column => column !== 'id' && column !== 'user_id' && column !== 'created_at' && column !== 'updated_at'
    );
    downloadCSV([], columns, `${modal}_create_template`);
  };

  const handleColumnSelectionChange = (column: string) => {
    setSelectedColumns((prev) =>
      prev.includes(column) ? prev.filter(col => col !== column) : [...prev, column]
    );
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

      {operation === 'create' && modal && (
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
          <label htmlFor="file" className="block text-sm text-yellow-100/50 mb-2">upload_csv</label>
          <input
            type="file"
            id="file"
            onChange={(e) => setFile(e.target.files ? e.target.files[0] : null)}
            className="block w-full text-yellow-100/50 bg-black border border-yellow-100/30 rounded-lg p-2.5"
          />
        </div>
      )}


{isColumnSelectionVisible && (
  <div className="flex flex-col space-y-2 mb-4">
    <label className="block text-sm text-yellow-100/50 mb-2">confirm_columns_to_update</label>
    {columns.filter(column => column !== 'id').map((filteredColumn) => (
      <label key={filteredColumn} className="flex items-center cursor-pointer">
        <input
          type="checkbox"
          id={filteredColumn}
          value={filteredColumn}
          onChange={() => handleColumnSelectionChange(filteredColumn)}
          checked={selectedColumns.includes(filteredColumn)}
          className="h-5 w-5 cursor-pointer appearance-none rounded-md border border-yellow-100/30 transition-all checked:bg-yellow-100/50 checked:border-none"
        />
        <span className="ml-3 text-yellow-100/50">{filteredColumn}</span>
      </label>
    ))}
  </div>
)}


      <div className="text-right">
        <button
          type="submit"
          className="bg-yellow-100 text-black py-2.5 px-6 rounded-lg mt-4"
          disabled={operation === 'update' && isColumnSelectionVisible && selectedColumns.length === 0}
        >
          Submit
        </button>
      </div>
    </form>
  );
};

export default BulkOperationsForm;

