import React, { useState, useEffect, useMemo, useCallback } from 'react';
import CreateModal from './CreateModal';
import EditModal from './EditModal';
import FilterInput from './FilterInput';
import QueryInput from './QueryInput';
import modalConfig from './modalConfig';
import { evaluateFilter, filterAndSortRows } from './filterUtils';
import { handleCreate, closeCreateModal, fetchData, handleDelete, handleEdit, closeEditModal } from './crudUtils';
import { handleQuerySubmit } from './queryUtils';
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
  const [editRowData, setEditRowData] = useState<any[]>([]);
  const [filterQuery, setFilterQuery] = useState('');
  const [queryInput, setQueryInput] = useState('');
  const [useQueryInput, setUseQueryInput] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);

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

  const handleQueryInputChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    setQueryInput(event.target.value);
  }, []);

  const handleQueryKeyPress = useCallback(
    (event: React.KeyboardEvent<HTMLInputElement>) => {
      if (event.key === 'Enter') {
        handleQuerySubmit(apiHost, modal, queryInput, setData, setQueryError);
      }
    },
    [apiHost, modal, queryInput]
  );

  const filteredData = useMemo(() => {
    return filterAndSortRows(data, filterQuery, columns);
  }, [data, filterQuery, columns]);

  const modalConfiguration = modalConfig[modal];

  if (!modalConfiguration) {
    return <div>Loading...</div>;
  }

  const columnIndices = modalConfiguration.scopes.read.map((col: string) => columns.indexOf(col));

  return (
    <div className="bg-black border border-yellow-100/30 rounded-lg text-yellow-100 p-4">
      <div className="flex justify-between mb-4">
        <div className="flex items-center w-full">
          <label className="flex items-center cursor-pointer">
            <div className="relative">
              <input
                type="checkbox"
                checked={useQueryInput}
                onChange={() => setUseQueryInput(!useQueryInput)}
                className="sr-only"
              />
              <div className="block bg-black w-14 h-10 rounded-lg border border-yellow-100/30"></div>
              <div
                className={`absolute left-1 top-1 bg-black border border-yellow-100/50 w-6 h-6 rounded-full transition-transform transform ${
                  useQueryInput ? 'translate-x-6 translate-y-2' : ''
                }`}
              ></div>
            </div>
            </label>
          {useQueryInput ? (
            <QueryInput
              queryInput={queryInput}
              handleQueryInputChange={handleQueryInputChange}
              handleQueryKeyPress={handleQueryKeyPress}
              queryError={queryError}
            />
          ) : (
            <FilterInput filterQuery={filterQuery} handleFilterChange={handleFilterChange} />
          )}
        </div>
        {modalConfiguration.scopes.create && (
          <button
            onClick={() => handleCreate(setCreateModalOpen)}
            className="bg-black border border-yellow-100/30 text-yellow-100/80 hover:bg-yellow-100/80 hover:text-black py-2 px-4 rounded-lg text-sm"
          >
            Create
          </button>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-yellow-100/30">
          <thead>
            <tr>
              {modalConfiguration.scopes.read.map((col: string, colIndex: number) => (
                <th
                  key={`col-${colIndex}`}
                  className="px-3 py-3 text-left text-xs font-medium text-yellow-100 tracking-wider"
                >
                  {col}
                </th>
              ))}
              <th className="px-3 py-3 text-left text-xs font-medium text-yellow-100 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody>
            {filteredData.map((row, rowIndex) => {
              return (
                <tr key={`row-${rowIndex}`} className="bg-black text-yellow-100/70 hover:bg-yellow-100/80 hover:text-black">
                  {columnIndices.map((colIndex, cellIndex) => {
                    const cellValue = row[colIndex];
                    return (
                      <td
                        key={`cell-${rowIndex}-${cellIndex}`}
                        className="px-3 py-2 whitespace-nowrap text-sm"
                      >
                        {typeof cellValue === 'string' && isValidUrl(cellValue) ? (
                          <button
                            onClick={() => window.open(cellValue, '_blank')}
                            className="bg-black border border-yellow-100/30 text-yellow-100/50 hover:bg-yellow-100/70 hover:text-black hover:border-black py-1 px-2 rounded-lg"
                          >
                            Open URL
                          </button>
                        ) : (
                          typeof cellValue === 'string' && !isNaN(Date.parse(cellValue)) ? formatDateTime(cellValue) : cellValue
                        )}
                      </td>
                    );
                  })}
                  <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-300">
                    <button
                      onClick={() => handleEdit(row, setEditRowData, setEditModalOpen)}
                      className="bg-black border border-yellow-100/30 text-yellow-100/50 hover:bg-yellow-100/70 hover:text-black hover:border-black py-1 px-2 rounded-lg mr-2"
                    >
                      Edit
                    </button>
                    {modalConfiguration.scopes.delete && (
                      <button
                        onClick={() => handleDelete(apiHost, modal, row[0], row[1], data, setData)}
                        className="bg-black border border-yellow-100/30 text-yellow-100/50 hover:bg-yellow-100/70 hover:text-black hover:border-black py-1 px-2 rounded-lg"
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

