import React from 'react';

interface SearchInputProps {
  searchInput: string;
  handleSearchInputChange: (event: React.ChangeEvent<HTMLInputElement>) => void;
  handleSearchKeyPress: (event: React.KeyboardEvent<HTMLInputElement>) => void;
  searchError: string | null;
}

const SearchInput: React.FC<SearchInputProps> = ({
  searchInput,
  handleSearchInputChange,
  handleSearchKeyPress,
  searchError,
}) => {
  return (
      <input
        type="text"
        value={searchInput}
        onChange={handleSearchInputChange}
        onKeyPress={handleSearchKeyPress}
        placeholder="SEARCH MODE (fetches fresh data) ..."
        className="bg-black border border-yellow-100/30 text-yellow-100 px-4 py-2 rounded-lg w-full mx-4 text-sm placeholder-yellow-100/50"
      />
  );
};

export default SearchInput;

