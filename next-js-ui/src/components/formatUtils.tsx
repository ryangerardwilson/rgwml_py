export const formatDateTime = (dateTime: string): string => {
  // Check if the input is a simple number
  if (!isNaN(Number(dateTime))) {
    return dateTime;
  }

  const date = new Date(dateTime);
  if (isNaN(date.getTime())) {
    // Return the original value if the input is not a valid date string
    return dateTime;
  }
  
  const year = date.getFullYear();
  const month = ('0' + (date.getMonth() + 1)).slice(-2);
  const day = ('0' + date.getDate()).slice(-2);
  const hours = ('0' + date.getHours()).slice(-2);
  const minutes = ('0' + date.getMinutes()).slice(-2);
  const seconds = ('0' + date.getSeconds()).slice(-2);
  
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
};



  export const isValidUrl = (url: string) => {
    try {
      new URL(url);
      return true;
    } catch (_) {
      return false;
    }
  };

