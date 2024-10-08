// src/utils/bulkOperationsUtils.ts
import { downloadCSV } from './downloadUtils';
import Papa from 'papaparse';

export async function handleReadOperation(apiHost: string, modal: string, timeLimit: string) {
  try {
    const response = await fetch(`${apiHost}bulk_read/${modal}/${timeLimit}`);
    const responseData = await response.json();
    const { columns, data } = responseData;
    downloadCSV(data, columns, 'bulk_read');
  } catch (error) {
    console.error('There was an error!', error);
  }
}

export async function handleCreateOperation(apiHost: string, modal: string, file: File, userId: string | undefined) {
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

    // Filter out unwanted columns
    const unwantedColumns = ['id', 'user_id', 'created_at', 'updated_at'];
    const columns = Object.keys(firstRow).filter(col => !unwantedColumns.includes(col));
    
    // Filter out unwanted data from each row
    const bulkValues = parsedData.data.map((row: any) => 
      columns.map(column => row[column])
    );

    const payload = {
      user_id: userId,
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
      window.location.reload();  // Refresh the page on success
    } else {
      alert(`Error: ${result.error}`);
    }
  } catch (error) {
    console.error('There was an error uploading the data!', error);
    alert('There was an error uploading the data.');
  }
}



export async function handleUpdateOperation(apiHost: string, modal: string, file: File | null, userId: string | undefined, selectedColumns: string[]) {
  if (file) {
    try {
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

      const firstRow = parsedData.data[0];
      if (!firstRow) {
        alert('The file appears to be empty or improperly formatted.');
        return;
      }

      const columns = Object.keys(firstRow);
      selectedColumns.push('id');

      const filteredData = parsedData.data.map((row: any) =>
        selectedColumns.reduce((acc: any, col: string) => {
          acc[col] = row[col];
          return acc;
        }, {})
      );

      const payload = {
        user_id: userId,
        columns: selectedColumns,
        data: filteredData // Include the whole row with 'id' field
      };

      console.log('159');
      console.log(payload);

      const response = await fetch(`${apiHost}bulk_update/${modal}`, {
        method: 'PUT', // Use PUT instead of POST
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      const result = await response.json();
      if (response.ok) {
        alert('Data updated successfully');
      } else {
        alert(`Error: ${result.error}`);
      }
    } catch (error) {
      console.error('There was an error updating the data!', error);
    }
  }
}

export async function handleDeleteOperation(apiHost: string, modal: string, userId: string | undefined, ids: number[]) {
  if (ids.length > 0 && userId) {
    try {
      const payload = {
        user_id: userId,
        ids: ids,
      };

      const response = await fetch(`${apiHost}bulk_delete/${modal}`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      const result = await response.json();
      if (response.ok) {
        alert('Data deleted successfully');
      } else {
        alert(`Error: ${result.error}`);
      }
    } catch (error) {
      console.error('There was an error deleting the data!', error);
      alert('There was an error deleting the data!');
    }
  } else {
    alert('User ID and IDs to delete are required.');
  }
}

