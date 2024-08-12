import React from 'react';

interface QueryInputProps {
  queryInput: string;
  handleQueryInputChange: (event: React.ChangeEvent<HTMLInputElement>) => void;
  handleQueryKeyPress: (event: React.KeyboardEvent<HTMLInputElement>) => void;
  queryError: string | null;
}

const QueryInput: React.FC<QueryInputProps> = ({
  queryInput,
  handleQueryInputChange,
  handleQueryKeyPress,
  queryError,
}) => {
  return (
      <input
        type="text"
        value={queryInput}
        onChange={handleQueryInputChange}
        onKeyPress={handleQueryKeyPress}
        placeholder="QUERY MODE (fetches fresh data) ..."
        className="bg-black border border-yellow-100/30 text-yellow-100 px-4 py-2 rounded-lg w-full mx-4 text-sm placeholder-yellow-100/50"
      />
  );
};

export default QueryInput;