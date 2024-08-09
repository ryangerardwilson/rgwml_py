export const downloadCSV = (data: any[], columns: string[], filename: string) => {
  const csvRows: string[] = [];
  const headers = columns.join(',');
  csvRows.push(headers);

  data.forEach(row => {
    const values = row.map((cellValue: any) => `"${cellValue}"`); // Enclose each cell value in quotes to handle commas within the values
    csvRows.push(values.join(','));
  });

  const csvContent = "data:text/csv;charset=utf-8," + encodeURIComponent(csvRows.join("\n"));

  // Get the current timestamp
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const hours = String(now.getHours()).padStart(2, '0');
  const minutes = String(now.getMinutes()).padStart(2, '0');
  const seconds = String(now.getSeconds()).padStart(2, '0');

  const timestamp = `${year}${month}${day}_${hours}${minutes}${seconds}`;
  const timestampedFilename = `${filename}_${timestamp}.csv`;

  const link = document.createElement('a');
  link.setAttribute('href', csvContent);
  link.setAttribute('download', timestampedFilename);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};


