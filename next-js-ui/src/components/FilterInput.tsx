import React from 'react';

interface FilterInputProps {
  filterQuery: string;
  handleFilterChange: (event: React.ChangeEvent<HTMLInputElement>) => void;
}

const FilterInput: React.FC<FilterInputProps> = ({ filterQuery, handleFilterChange }) => {
  return (
    <input
      type="text"
      value={filterQuery}
      onChange={handleFilterChange}
      placeholder="FILTER MODE (filters existing data) ..."
      className="bg-black border border-yellow-100/30 text-yellow-100 px-4 py-2 rounded-lg w-full mx-4 text-sm placeholder-yellow-100/50"
    />
  );
};

export default FilterInput;

