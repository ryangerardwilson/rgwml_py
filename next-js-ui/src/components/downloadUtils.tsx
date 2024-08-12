export const downloadCSV = (data: any[], columns: string[], filename: string) => {
  const csvRows: string[] = [];
  const headers = columns.join(',');
  csvRows.push(headers);

  const escapeCellValue = (value: any): string => {
    if (value === null || value === undefined) return '';

    let cellValue = value;
    if (typeof value === 'object') {
      cellValue = JSON.stringify(value);
    } else {
      cellValue = String(value);
    }

    // Escape double quotes by replacing " with ""
    cellValue = cellValue.replace(/"/g, '""');

    // Enclose the cell in double quotes if it contains a comma, double quotes, or a newline
    if (/[",\n]/.test(cellValue)) {
      cellValue = `"${cellValue}"`;
    }

    return cellValue;
  };

  data.forEach(row => {
    const values = row.map((cellValue: any) => escapeCellValue(cellValue));
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

