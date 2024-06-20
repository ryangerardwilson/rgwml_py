export const evaluateFilter = (row: any, query: string, columns: string[]): boolean => {
  const regex = /(\w+)\s*(CONTAINS|STARTS_WITH|==|>|<|>=|<=)\s*'?([^']*)'?\s*(AND\s+|$)/gi;
  const conditions = query.match(regex);

  if (!conditions) return true;

  for (const condition of conditions) {
    const conditionRegex = /(\w+)\s*(CONTAINS|STARTS_WITH|==|>|<|>=|<=)\s*'?([^']*)'?\s*(AND\s+|$)/i;
    const match = condition.match(conditionRegex);

    if (!match) continue;

    const [, column, operator, value] = match;
    const columnIndex = columns.indexOf(column);

    if (columnIndex === -1) return false;

    const cellValue = row[columnIndex];
    const numericValue = parseFloat(value);
    const isNumericComparison = !isNaN(numericValue);

    let conditionResult = false;
    switch (operator) {
      case 'CONTAINS':
        conditionResult = cellValue.toString().toLowerCase().includes(value.toLowerCase());
        break;
      case 'STARTS_WITH':
        conditionResult = cellValue.toString().toLowerCase().startsWith(value.toLowerCase());
        break;
      case '==':
        conditionResult = cellValue.toString() === value;
        break;
      case '>':
        if (isNumericComparison) {
          conditionResult = parseFloat(cellValue) > numericValue;
        } else {
          conditionResult = new Date(cellValue) > new Date(value);
        }
        break;
      case '<':
        if (isNumericComparison) {
          conditionResult = parseFloat(cellValue) < numericValue;
        } else {
          conditionResult = new Date(cellValue) < new Date(value);
        }
        break;
      case '>=':
        if (isNumericComparison) {
          conditionResult = parseFloat(cellValue) >= numericValue;
        } else {
          conditionResult = new Date(cellValue) >= new Date(value);
        }
        break;
      case '<=':
        if (isNumericComparison) {
          conditionResult = parseFloat(cellValue) <= numericValue;
        } else {
          conditionResult = new Date(cellValue) <= new Date(value);
        }
        break;
      default:
        conditionResult = true;
    }

    if (!conditionResult) {
      return false;
    }
  }

  return true;
};

export const filterAndSortRows = (rows: any[], query: string, columns: string[]): any[] => {
  const orderByRegex = /ORDER BY (\w+) (ASC|DESC)/i;
  const orderMatch = query.match(orderByRegex);
  let orderColumn = null;
  let orderDirection = null;

  if (orderMatch) {
    orderColumn = orderMatch[1];
    orderDirection = orderMatch[2].toUpperCase();
    query = query.replace(orderMatch[0], '').trim(); // Remove ORDER BY clause from the query
  }

  const filteredRows = rows.filter(row => evaluateFilter(row, query, columns));

  if (orderColumn && orderDirection) {
    const orderColumnIndex = columns.indexOf(orderColumn);

    if (orderColumnIndex !== -1) {
      filteredRows.sort((a, b) => {
        const valueA = a[orderColumnIndex];
        const valueB = b[orderColumnIndex];

        if (orderDirection === 'ASC') {
          if (valueA < valueB) return -1;
          if (valueA > valueB) return 1;
          return 0;
        } else {
          if (valueA > valueB) return -1;
          if (valueA < valueB) return 1;
          return 0;
        }
      });
    }
  }

  return filteredRows;
};


