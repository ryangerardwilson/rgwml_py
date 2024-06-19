import React, { useState, useEffect } from 'react';

interface CreateModalProps {
  modalName: string;
  apiHost: string;
  columns: string[];
  onClose: () => void;
}

const CreateModal: React.FC<CreateModalProps> = ({ modalName, apiHost, columns, onClose }) => {
  const [formData, setFormData] = useState<{ [key: string]: any }>({});
  const [userId, setUserId] = useState<string | null>(null);

  useEffect(() => {
    // Extract user_id from cookies
    const cookies = document.cookie.split(';').reduce((acc, cookie) => {
      const [key, value] = cookie.trim().split('=');
      acc[key] = value;
      return acc;
    }, {} as { [key: string]: string });

    setUserId(cookies.user_id || null);
  }, []);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  const handleSubmit = async () => {
    try {
      const dataToSubmit = { ...formData, user_id: userId };
      console.log('35',dataToSubmit);
      const response = await fetch(`${apiHost}create/${modalName}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(dataToSubmit)
      });
      if (response.ok) {
        alert('Record created successfully');
        onClose();
	 window.location.reload();
      } else {
        alert('Failed to create record');
      }
    } catch (error) {
      console.error('Error creating record:', error);
    }
  };

  const filteredColumns = columns.filter(column => !['id', 'created_at', 'updated_at', 'user_id'].includes(column));

  return (
    <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
      <div className="bg-gray-800 p-8 rounded-lg shadow-lg text-white w-96">
        <h2 className="text-2xl mb-4">Create New {modalName.charAt(0).toUpperCase() + modalName.slice(1)}</h2>
        {filteredColumns.map((column) => (
          <input
            key={column}
            type="text"
            name={column}
            placeholder={column}
            onChange={handleChange}
            className="bg-gray-700 p-2 rounded mb-4 w-full"
          />
        ))}
        <div className="flex justify-end">
          <button onClick={onClose} className="bg-red-500 hover:bg-red-700 text-white py-2 px-4 rounded mr-2">Cancel</button>
          <button onClick={handleSubmit} className="bg-green-500 hover:bg-green-700 text-white py-2 px-4 rounded">Create</button>
        </div>
      </div>
    </div>
  );
};

export default CreateModal;

