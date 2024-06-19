import React, { useState, useEffect } from 'react';
import CreateModal from './CreateModal';
import EditModal from './EditModal';

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
  const [filterQuery, setFilterQuery] = useState(''); // State for the filter query

  useEffect(() => {
    setData(initialData);
  }, [initialData]);

  useEffect(() => {
    if (!initialData.length) {
      const fetchData = async () => {
        try {
          const response = await fetch(`${apiHost}read/${modal}`);
          const result = await response.json();
          setData(result.data || []);
        } catch (error) {
          console.error('Error fetching data:', error);
          setData([]);
        }
      };
      fetchData();
    }
  }, [apiHost, modal, initialData]);

  const handleDelete = async (id: number, userId: number) => {
    try {
      const response = await fetch(`${apiHost}delete/${modal}/${id}`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ user_id: userId }),
      });
      const result = await response.json();
      if (result.status === 'success') {
        setData(data.filter((row: any) => row[0] !== id)); // Assuming the first column is the ID
      }
    } catch (error) {
      console.error('Error deleting data:', error);
    }
  };

  const handleEdit = (row: { [key: string]: any }) => {
    setEditRowData(row);
    setEditModalOpen(true);
  };

  const handleCreate = () => {
    setCreateModalOpen(true);
  };

  const closeCreateModal = () => setCreateModalOpen(false);

  const closeEditModal = (updatedData: any[] | null) => {
    if (updatedData) {
      // Update the rowData with the new data
      setData(prevData => prevData.map(row => row[0] === updatedData[0] ? updatedData : row));
    }
    setEditModalOpen(false);
  };

  const handleFilterChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setFilterQuery(event.target.value);
  };

  const evaluateFilter = (row: any, query: string): boolean => {
    const regex = /(\w+)\s*(CONTAINS|STARTS_WITH|==|>|<|>=|<=)\s*'([^']*)'/i;
    const match = query.match(regex);

    if (!match) return true;

    const [, column, operator, value] = match;
    const columnIndex = columns.indexOf(column);

    if (columnIndex === -1) return false;

    const cellValue = row[columnIndex];

    switch (operator) {
      case 'CONTAINS':
        return cellValue.toString().toLowerCase().includes(value.toLowerCase());
      case 'STARTS_WITH':
        return cellValue.toString().toLowerCase().startsWith(value.toLowerCase());
      case '==':
        return cellValue.toString() === value;
      case '>':
        return new Date(cellValue) > new Date(value) || parseFloat(cellValue) > parseFloat(value);
      case '<':
        return new Date(cellValue) < new Date(value) || parseFloat(cellValue) < parseFloat(value);
      case '>=':
        return new Date(cellValue) >= new Date(value) || parseFloat(cellValue) >= parseFloat(value);
      case '<=':
        return new Date(cellValue) <= new Date(value) || parseFloat(cellValue) <= parseFloat(value);
      default:
        return true;
    }
  };

  const filteredData = data.filter(row => evaluateFilter(row, filterQuery));

  const formatDateTime = (dateTime: string) => {
    const date = new Date(dateTime);
    const year = date.getFullYear();
    const month = ('0' + (date.getMonth() + 1)).slice(-2);
    const day = ('0' + date.getDate()).slice(-2);
    const hours = ('0' + date.getHours()).slice(-2);
    const minutes = ('0' + date.getMinutes()).slice(-2);
    const seconds = ('0' + date.getSeconds()).slice(-2);
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  };

  return (
    <div className="bg-gray-900 text-white p-4">
      <div className="flex justify-between mb-4">
        <input
          type="text"
          value={filterQuery}
          onChange={handleFilterChange}
          placeholder="Filter [==, >, >=, <, <=, CONTAINS, STARTS_WITH] ..."
          className="bg-gray-800 text-white px-4 py-2 rounded w-1/3"
        />
        <button
          onClick={handleCreate}
          className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
        >
          Create
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-700">
          <thead>
            <tr>
              {columns.map((col, colIndex) => (
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
            {filteredData.map((row, rowIndex) => (
              <tr key={`row-${rowIndex}`} className="bg-gray-800">
                {row.map((cell: any, cellIndex: number) => (
                  <td
                    key={`cell-${rowIndex}-${cellIndex}`}
                    className="px-3 py-4 whitespace-nowrap text-sm text-gray-300"
                  >
                    {typeof cell === 'string' && !isNaN(Date.parse(cell)) ? formatDateTime(cell) : cell}
                  </td>
                ))}
                <td className="px-3 py-4 whitespace-nowrap text-sm text-gray-300">
                  <button
                    onClick={() => handleEdit(row)}
                    className="bg-yellow-500 hover:bg-yellow-700 text-white font-bold py-1 px-2 rounded mr-2"
                  >
                    Edit
                  </button>
                  <button
                    onClick={() => handleDelete(row[0], row.user_id)} // Pass the user_id here
                    className="bg-red-500 hover:bg-red-700 text-white font-bold py-1 px-2 rounded"
                  >
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {isCreateModalOpen && <CreateModal modalName={modal} apiHost={apiHost} columns={columns} onClose={closeCreateModal} />}
      {isEditModalOpen && editRowData && <EditModal modalName={modal} apiHost={apiHost} columns={columns} rowData={editRowData} onClose={closeEditModal} />}
    </div>
  );
};

export default DynamicTable;

