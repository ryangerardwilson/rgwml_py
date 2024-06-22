export const handleQuerySubmit = async (
  apiHost: string,
  modal: string,
  queryInput: string,
  setData: React.Dispatch<React.SetStateAction<any[]>>,
  setQueryError: React.Dispatch<React.SetStateAction<string | null>>
) => {
  try {
    const response = await fetch(`${apiHost}/query/${modal}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query_string: queryInput.trim() }),
    });
    const result = await response.json();
    if (response.ok) {
      setData(result.data);
      setQueryError(null);
    } else {
      console.error('Error fetching query results:', result);
      setQueryError(result.error || 'Unknown error occurred');
    }
  } catch (error) {
    console.error('Error fetching query results:', error);
    if (error instanceof Error) {
      setQueryError(error.message || 'Unknown error occurred');
    } else {
      setQueryError('Unknown error occurred');
    }
  }
};

