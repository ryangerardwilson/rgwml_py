export const downloadCSV = (data: any[], columns: string[], filename: string) => {
  const csvRows: string[] = [];
  const headers = columns.join(',');
  csvRows.push(headers);

  data.forEach(row => {
    const values = row.map((cellValue: any) => `"${cellValue}"`); // Enclose each cell value in quotes to handle commas within the values
    csvRows.push(values.join(','));
  });






  const csvContent = "data:text/csv;charset=utf-8," + encodeURIComponent(csvRows.join("\n"));



  const link = document.createElement('a');
  link.setAttribute('href', csvContent);
  link.setAttribute('download', `${filename}.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};

