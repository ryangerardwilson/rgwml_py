import React, { useState, useEffect, useMemo, useCallback } from 'react';
import CreateModal from './CreateModal';
import EditModal from './EditModal';
import FilterInput from './FilterInput';
import modalConfig from './modalConfig';
import { evaluateFilter, filterAndSortRows } from './filterUtils';
import { handleCreate, closeCreateModal, fetchData, handleDelete, handleEdit, closeEditModal } from './crudUtils';
import { isValidUrl, formatDateTime } from './formatUtils';

interface DynamicTableProps {
  apiHost: string;
  modal: string;
  columns: string[];
  data: any[];
}

const DynamicTable: React.FC<DynamicTableProps> = ({ apiHost, modal, columns, data: initialData }) => {
  const [data, setData] = useState<any[]>(initialData);
  const [isCreateModalOpen, setCreateModalOpen] = useState(false);
  const [isEditModalOpen, setEditModalOpen] = useState(false);
  const [editRowData, setEditRowData] = useState<{ [key: string]: any } | null>(null);
  const [filterQuery, setFilterQuery] = useState('');

  useEffect(() => {
    setData(initialData);
  }, [initialData]);

  useEffect(() => {
    if (!initialData.length) {
      fetchData(apiHost, modal, setData);
    }
  }, [apiHost, modal, initialData]);

  const handleFilterChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    setFilterQuery(event.target.value);
  }, []);

  const filteredData = useMemo(() => {
    return filterAndSortRows(data, filterQuery, columns);
  }, [data, filterQuery, columns]);

  const modalConfiguration = modalConfig[modal];

  if (!modalConfiguration) {
    return <div>Loading...</div>;
  }

  const columnIndices = modalConfiguration.scopes.read.map((col: string) => columns.indexOf(col));

  return (
    <div className="bg-gray-900 text-white p-4">
      <div className="flex justify-between mb-4">
        <FilterInput filterQuery={filterQuery} handleFilterChange={handleFilterChange} />

        {modalConfiguration.scopes.create && (
          <button
            onClick={() => handleCreate(setCreateModalOpen)}
            className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
          >
            Create
          </button>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-700">
          <thead>
            <tr>
              {modalConfiguration.scopes.read.map((col: string, colIndex: number) => (
                <th
                  key={`col-${colIndex}`}
                  className="px-3 py-3 text-left text-xs font-medium text-gray-300 tracking-wider"
                >
                  {col}
                </th>
              ))}
              <th className="px-3 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody>
            {filteredData.map((row, rowIndex) => {
              return (
                <tr key={`row-${rowIndex}`} className="bg-gray-800">
                  {columnIndices.map((colIndex, cellIndex) => {
                    const cellValue = row[colIndex];
                    return (
                      <td
                        key={`cell-${rowIndex}-${cellIndex}`}
                        className="px-3 py-4 whitespace-nowrap text-sm text-gray-300"
                      >
                        {typeof cellValue === 'string' && isValidUrl(cellValue) ? (
                          <button
                            onClick={() => window.open(cellValue, '_blank')}
                            className="bg-green-500 hover:bg-green-700 text-white font-bold py-1 px-2 rounded"
                          >
                            Open URL
                          </button>
                        ) : (
                          typeof cellValue === 'string' && !isNaN(Date.parse(cellValue)) ? formatDateTime(cellValue) : cellValue
                        )}
                      </td>
                    );
                  })}
                  <td className="px-3 py-4 whitespace-nowrap text-sm text-gray-300">
                    <button
                      onClick={() => handleEdit(row, setEditRowData, setEditModalOpen)}
                      className="bg-yellow-500 hover:bg-yellow-700 text-white font-bold py-1 px-2 rounded mr-2"
                    >
                      Edit
                    </button>
                    {modalConfiguration.scopes.delete && (
                      <button
                        onClick={() => handleDelete(apiHost, modal, row[0], row[1], data, setData)}
                        className="bg-red-500 hover:bg-red-700 text-white font-bold py-1 px-2 rounded"
                      >
                        Delete
                      </button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {isCreateModalOpen && (
        <CreateModal
          modalName={modal}
          apiHost={apiHost}
          columns={columns}
          onClose={() => closeCreateModal(setCreateModalOpen)}
        />
      )}

      {isEditModalOpen && editRowData && (
        <EditModal
          modalName={modal}
          apiHost={apiHost}
          columns={columns}
          rowData={editRowData}
          onClose={(updatedData) => closeEditModal(updatedData, columns, setData, setEditModalOpen)}
        />
      )}
    </div>
  );
};

export default DynamicTable;

