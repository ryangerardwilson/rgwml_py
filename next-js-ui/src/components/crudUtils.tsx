
export const handleCreate = (setCreateModalOpen: (open: boolean) => void) => {
  setCreateModalOpen(true);
};

export const closeCreateModal = (setCreateModalOpen: (open: boolean) => void) => {
  setCreateModalOpen(false);
};

export const fetchData = async (apiHost: string, modal: string, setData: React.Dispatch<React.SetStateAction<any[]>>) => {
  try {
    const response = await fetch(`${apiHost}read/${modal}`);
    const result = await response.json();
    setData(result.data || []);
  } catch (error) {
    setData([]);
  }
};

export const handleDelete = async (apiHost: string, modal: string, id: number, userId: number, data: any[], setData: React.Dispatch<React.SetStateAction<any[]>>) => {
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
      setData(data.filter((row: any) => row[0] !== id));
    }
  } catch (error) {
    console.error('Error deleting data:', error);
  }
};

export const handleEdit = (row: { [key: string]: any }, setEditRowData: React.Dispatch<React.SetStateAction<{ [key: string]: any } | null>>, setEditModalOpen: React.Dispatch<React.SetStateAction<boolean>>) => {
  setEditRowData(row);
  setEditModalOpen(true);
};

export const closeEditModal = (
  updatedData: { [key: string]: any } | null,
  columns: string[],
  setData: React.Dispatch<React.SetStateAction<any[]>>,
  setEditModalOpen: React.Dispatch<React.SetStateAction<boolean>>
) => {
  if (updatedData) {
    const updatedId = updatedData.id; // Assuming the first column is the unique identifier

    setData(prevData => {
      const newData = prevData.map(row => {
        if (row[0] === updatedId) {
          const updatedRow = columns.map(column => {
            return updatedData[column] !== undefined ? updatedData[column] : null;
          });
          return updatedRow;
        }
        return row;
      });
      return newData;
    });
  }
  setEditModalOpen(false);
};
