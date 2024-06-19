import React, { useState, useEffect } from 'react';

interface EditModalProps {
  modalName: string;
  apiHost: string;
  columns: string[];
  rowData: any[];
  onClose: (updatedData: any[]) => void; // Modify onClose to accept updated data
}

const EditModal: React.FC<EditModalProps> = ({ modalName, apiHost, columns, rowData, onClose }) => {
  const [formData, setFormData] = useState<{ [key: string]: any }>({});

  useEffect(() => {
    // Convert rowData to an object with column names as keys
    const initialFormData = columns.reduce((acc, column, index) => {
      acc[column] = rowData[index];
      return acc;
    }, {} as { [key: string]: any });
    setFormData(initialFormData);
  }, [columns, rowData]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  const handleSubmit = async () => {
    try {
      const response = await fetch(`${apiHost}update/${modalName}/${formData.id}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(formData)
      });
      if (response.ok) {
        alert('Record updated successfully');
        onClose(Object.values(formData)); // Pass updated data to onClose
      } else {
        alert('Failed to update record');
      }
    } catch (error) {
      console.error('Error updating record:', error);
    }
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
      <div className="bg-gray-800 p-8 rounded-lg shadow-lg text-white w-96">
        <h2 className="text-2xl mb-4">
          Edit {modalName.charAt(0).toUpperCase() + modalName.slice(1)} ({rowData[0]})
        </h2>
        {columns.map((column) => (
          column !== 'created_at' && column !== 'updated_at' && column !== 'id' && column !== 'user_id' && (
            <div key={column} className="mb-4">
              <label className="block text-sm font-medium text-gray-300">{column}</label>
              <input
                type="text"
                name={column}
                value={formData[column] || ''}
                placeholder={column}
                onChange={handleChange}
                className="bg-gray-700 p-2 rounded w-full"
              />
            </div>
          )
        ))}
        <div className="flex justify-end">
          <button onClick={() => onClose(null)} className="bg-red-500 hover:bg-red-700 text-white py-2 px-4 rounded mr-2">Cancel</button>
          <button onClick={handleSubmit} className="bg-green-500 hover:bg-green-700 text-white py-2 px-4 rounded">Update</button>
        </div>
      </div>
    </div>
  );
};

export default EditModal;

