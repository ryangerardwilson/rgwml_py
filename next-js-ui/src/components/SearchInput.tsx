import React from 'react';

interface SearchInputProps {
  searchInput: string;
  handleSearchInputChange: (event: React.ChangeEvent<HTMLInputElement>) => void;
  searchError: string | null;
}

const SearchInput: React.FC<SearchInputProps> = ({
  searchInput,
  handleSearchInputChange,
  searchError,
}) => {
  return (
    <div className="w-full flex flex-col">
      <input
        type="text"
        value={searchInput}
        onChange={handleSearchInputChange}
        placeholder="SEARCH MODE (fetches fresh data) ..."
        className="bg-black border border-yellow-100/30 text-yellow-100 px-4 py-2 rounded-lg w-full text-sm placeholder-yellow-100/50"
      />
      {searchError && <div className="text-red-500 mt-2 text-sm">{searchError}</div>}
    </div>
  );
};

export default SearchInput;

