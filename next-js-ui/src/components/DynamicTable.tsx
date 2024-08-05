import React, { useState, useEffect, useMemo, useCallback } from 'react';
import CreateModal from './CreateModal';
import EditModal from './EditModal';
import SearchInput from './SearchInput';
import modalConfig from './modalConfig';
import { handleCreate, closeCreateModal, fetchData, handleDelete, handleEdit, closeEditModal } from './crudUtils';
import { isValidUrl, formatDateTime } from './formatUtils';
import { downloadCSV } from './downloadUtils';

interface DynamicTableProps {
  apiHost: string;
  modal: string;
  columns: string[];
  data: any[];
}

const DynamicTable: React.FC<DynamicTableProps> = ({ apiHost, modal, columns, data: initialData }) => {
  const [data, setData] = useState<any[]>(initialData);
  const [filteredData, setFilteredData] = useState<any[]>(initialData);
  const [isCreateModalOpen, setCreateModalOpen] = useState(false);
  const [isEditModalOpen, setEditModalOpen] = useState(false);
  const [editRowData, setEditRowData] = useState<any[]>([]);
  const [searchInput, setSearchInput] = useState('');
  const [searchError, setSearchError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string | null>(null);

  const readRoutes = useMemo(() => modalConfig[modal]?.read_routes || [], [modal]);

  useEffect(() => {
    if (readRoutes.length > 0) {
      setActiveTab(readRoutes[0]);
    }
  }, [readRoutes]);

  useEffect(() => {
    if (activeTab) {
      fetchData(apiHost, modal, activeTab, (newData) => {
        setData(newData);
        setFilteredData(newData);
      });
    }
  }, [apiHost, modal, activeTab]);

  const handleSearchInputChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchInput(event.target.value);
    const lowerCaseSearchInput = event.target.value.toLowerCase();

    if (!lowerCaseSearchInput) {
      setFilteredData(data);
    } else {
      const filtered = data.filter(row =>
        row.some((cell: unknown) =>
          cell !== null && cell !== undefined && cell.toString().toLowerCase().includes(lowerCaseSearchInput)
        )
      );
      setFilteredData(filtered);
    }
  }, [data]);

  const columnIndices = modalConfig[modal]?.scopes.read.map((col: string) => columns.indexOf(col)) || [];

  const copyToClipboard = (row: any, columnNames: string[]) => {
    const rowString = columnNames.map((col, index) => `${col}: ${row[index]}`).join('\n');
    navigator.clipboard.writeText(rowString).then(() => {
      alert('Row copied to clipboard');
    }).catch(err => {
      console.error('Could not copy text: ', err);
    });
  };

  return (
    <div className="bg-black border border-yellow-100/30 rounded-lg text-yellow-100 p-4 text-sm">
      <div className="mb-4">
        {readRoutes.length > 0 && (
          <div className="flex space-x-4">
            {readRoutes.map((route) => (
              <button
                key={route}
                className={`px-4 py-2 rounded ${activeTab === route ? 'border-t border-x border-yellow-100/50 rounded-lg' : 'bg-black text-yellow-100/50'}`}
                onClick={() => setActiveTab(route)}
              >
                {route}
              </button>
            ))}
          </div>
        )}
      </div>
      <div className="flex justify-between mb-4">
        <div className="flex items-center w-full">
          <SearchInput
            searchInput={searchInput}
            handleSearchInputChange={handleSearchInputChange}
            searchError={searchError}
          />
        </div>
        <button
          className="bg-black border border-yellow-100/30 text-yellow-100/80 hover:bg-yellow-100/80 hover:text-black py-2 px-4 mr-4 rounded-lg text-sm"
          onClick={() => downloadCSV(filteredData, modalConfig[modal]?.scopes.read, `${modal}_data`)}
        >
          CSV
        </button>
        {modalConfig[modal]?.scopes.create && (
          <button
            onClick={() => setCreateModalOpen(true)}
            className="bg-black border border-yellow-100/30 text-yellow-100/80 hover:bg-yellow-100/80 hover:text-black py-2 px-4 rounded-lg text-sm"
          >
            Create
          </button>
        )}
      </div>

      {/* Count of Rows */}
      <div className="mb-4 w-fill text-center italic">
        <p>*** Rows Fetched: {filteredData.length} ***</p>
      </div>

      <div className="overflow-x-auto h-96">
        <table className="table-fixed min-w-full divide-y divide-yellow-100/30">
          <thead className="bg-black sticky top-0">
            <tr>
              <th className="px-3 py-3 text-left text-xs font-medium text-yellow-100 uppercase tracking-wider">Actions</th>
              {modalConfig[modal]?.scopes.read.map((col: string, colIndex: number) => (
                <th
                  key={`col-${colIndex}`}
                  className="px-3 py-3 text-left text-xs font-medium text-yellow-100 w-96"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredData.map((row, rowIndex) => (
              <tr key={`row-${rowIndex}`} className="bg-black text-yellow-100/70 hover:bg-yellow-100/80 hover:text-black">
                <td className="px-3 py-2 break-words text-sm text-gray-300 min-w-64">
                  <button
                    onClick={() => handleEdit(row, setEditRowData, setEditModalOpen)}
                    className="bg-black border border-yellow-100/30 text-yellow-100/50 hover:bg-yellow-100/70 hover:text-black hover:border-black py-1 px-2 rounded-lg mr-2"
                  >
                    Edit
                  </button>
                  {modalConfig[modal]?.scopes.delete && (
                    <button
                      onClick={() => handleDelete(apiHost, modal, row[0], row[1])}
                      className="bg-black border border-yellow-100/30 text-yellow-100/50 hover:bg-yellow-100/70 hover:text-black hover:border-black py-1 px-2 rounded-lg mr-2"
                    >
                      Delete
                    </button>
                  )}
                  <button
                    onClick={() => copyToClipboard(row, modalConfig[modal]?.scopes.read)}
                    className="bg-black border border-yellow-100/30 text-yellow-100/50 hover:bg-yellow-100/70 hover:text-black hover:border-black py-1 px-2 rounded-lg"
                  >
                    Copy
                  </button>
                </td>
                {columnIndices.map((colIndex, cellIndex) => {
                  if (colIndex === -1) return null;
                  const cellValue = row[colIndex];
                  const truncatedValue = typeof cellValue === 'string' && cellValue.length > 250
                    ? `${cellValue.slice(0, 250)}...`
                    : cellValue;
                  const isLongCellValue = typeof cellValue === 'string' && cellValue.length > 15;

                  return (
                    <td
                      key={`cell-${rowIndex}-${cellIndex}`}
                      className={`px-3 py-2 break-words text-sm ${isLongCellValue ? 'min-w-96' : ''}`}
                    >
                      {typeof truncatedValue === 'string' && isValidUrl(truncatedValue) ? (
                        <button
                          onClick={() => window.open(truncatedValue, '_blank')}
                          className="bg-black border border-yellow-100/30 text-yellow-100/50 hover:bg-yellow-100/70 hover:text-black hover:border-black py-1 px-2 rounded-lg"
                        >
                          Open URL
                        </button>
                      ) : (
                        typeof truncatedValue === 'string' && !isNaN(Date.parse(truncatedValue)) ? formatDateTime(truncatedValue) : truncatedValue
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
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

