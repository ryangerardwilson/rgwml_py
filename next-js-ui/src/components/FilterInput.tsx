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
      placeholder="Query syntax: id > 70 AND Col7 == 'X' ORDER BY id DESC"
      className="bg-gray-800 text-white px-4 py-2 rounded w-full mr-4"
    />
  );
};

export default FilterInput;

