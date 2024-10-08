import React, { useState, useCallback, useEffect } from 'react';
import modalConfig from './modalConfig';
import { validateField, open_ai_quality_checks } from './validationUtils';

interface CreateModalProps {
  modalName: string;
  apiHost: string;
  columns: string[];
  onClose: () => void;
}

const CreateModal: React.FC<CreateModalProps> = ({ modalName, apiHost, columns, onClose }) => {
  const [formData, setFormData] = useState<{ [key: string]: any }>({});
  const [errors, setErrors] = useState<{ [key: string]: string | null }>({});
  const [dynamicOptions, setDynamicOptions] = useState<{ [key: string]: string[] }>({});
  const config = modalConfig[modalName];

  useEffect(() => {
    const initialData = columns.reduce((acc, col) => {
      acc[col] = '';
      return acc;
    }, {} as { [key: string]: any });
    setFormData(initialData);
  }, [columns]);

  const evalCondition = useCallback((condition: string) => {
    const conditionToEvaluate = condition.replace(/(\w+)/g, (match) => {
      if (formData.hasOwnProperty(match)) {
        return `formData['${match}']`;
      }
      return `'${match}'`;
    });
    try {
      const result = new Function('formData', `return ${conditionToEvaluate};`)(formData);
      return result;
    } catch (e) {
      console.error('Error evaluating condition:', condition, e);
      return false;
    }
  }, [formData]);

  const updateDynamicOptions = useCallback(() => {
    const newDynamicOptions: { [key: string]: string[] } = {};
    if (config.conditional_options) {
      for (const [field, conditions] of Object.entries(config.conditional_options)) {
        for (const conditionObj of conditions) {
          if (evalCondition(conditionObj.condition)) {
            newDynamicOptions[field] = conditionObj.options;
            break; // Stop checking other conditions if one matches
          }
        }
      }
    }
    setDynamicOptions(newDynamicOptions);
  }, [config, evalCondition]);

  useEffect(() => {
    updateDynamicOptions();
  }, [updateDynamicOptions]);

  const getCookie = (name: string): string | undefined => {
    const cookies = document.cookie.split(';').reduce((acc, cookie) => {
      const [key, value] = cookie.trim().split('=');
      acc[key] = value;
      return acc;
    }, {} as { [key: string]: string });
    return cookies[name];
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value, type } = e.target;

    setFormData((prevData) => {
      if (type === 'checkbox') {
        if (e.target instanceof HTMLInputElement) {
          const checked = e.target.checked;
          const currentValues = prevData[name] ? prevData[name].split(';') : [];
          const updatedValues = checked
            ? [...currentValues, value]
            : currentValues.filter((item: string) => item !== value);
          return { ...prevData, [name]: updatedValues.join(';') };
        }
      } else {
        return { ...prevData, [name]: value };
      }
    });

    if (config.validation_rules && config.validation_rules[name]) {
      const error = validateField(name, value, config.validation_rules[name]);
      setErrors((prevErrors) => ({ ...prevErrors, [name]: error }));
    }
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    let valid = true;
    const newErrors: { [key: string]: string | null } = {};

    if (config.validation_rules) {
      for (const field of Object.keys(config.validation_rules)) {
        const error = validateField(field, formData[field], config.validation_rules[field]);
        if (error) {
          valid = false;
          newErrors[field] = error;
        }
      }
    }

    if (config.ai_quality_checks) {
      for (const field of Object.keys(config.ai_quality_checks)) {
        const aiErrors = await open_ai_quality_checks(field, formData[field], config.ai_quality_checks[field]);
        if (aiErrors.length > 0) {
          valid = false;
          newErrors[field] = aiErrors.join(', ');
        }
      }
    }

    setErrors(newErrors);

    if (!valid) {
      alert('Please fix the validation errors.');
      return;
    }

    const user_id = getCookie('user_id');
    if (user_id) {
      formData['user_id'] = user_id;
    } else {
      console.error('User ID not found in cookies');
      onClose();
      return;
    }

    try {
      const response = await fetch(`${apiHost}create/${modalName}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData),
      });
      const result = await response.json();
      if (result.status === 'success') {
        alert('Record created successfully');
        onClose();
        window.location.reload();
      } else {
        console.error('Failed to create record:', result);
        onClose();
      }
    } catch (error) {
      console.error('Error creating record:', error);
      onClose();
    }
  };

  const isFieldEnabled = (field: string) => {
    if (!config.conditional_options || !config.conditional_options[field]) {
      return true;
    }
    return config.conditional_options[field].some((conditionObj: any) => evalCondition(conditionObj.condition));
  };

  const renderField = (col: string) => {
    const orTypeOptions = Object.entries(config.options).find(([key]) => key.startsWith(`${col}[OR]`));
    const xorTypeOptions = Object.entries(config.options).find(([key]) => key.startsWith(`${col}[XOR]`));

    if (orTypeOptions) {
      return (
        <div className="flex flex-col space-y-2">
          {orTypeOptions[1]?.map((option: string) => (
            <label key={option} className="flex items-center cursor-pointer">
              <input
                type="checkbox"
                name={col}
                value={option}
                checked={formData[col]?.split(';').includes(option) || false}
                onChange={handleChange}
                className="h-5 w-5 cursor-pointer appearance-none rounded-md border border-yellow-100/30 transition-all checked:bg-yellow-100/50 checked:border-none"
              />
              <span className="ml-3 text-yellow-100/50">{option}</span>
            </label>
          ))}
        </div>
      );
    } else if (xorTypeOptions) {
      return (
        <select
          name={col}
          value={formData[col] || ''}
          onChange={handleChange}
          className="bg-black text-yellow-100/50 px-3 py-2 rounded-lg border border-yellow-100/30 w-full text-sm"
        >
          <option value="" disabled>
            Select {col}
          </option>
          {xorTypeOptions[1]?.map((option: string) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      );
    } else if (dynamicOptions[col]) {
      return (
        <select
          name={col}
          value={formData[col] || ''}
          onChange={handleChange}
          className="bg-black text-yellow-100/50 px-3 py-2 rounded-lg border border-yellow-100/30 w-full text-sm"
        >
          <option value="" disabled>
            Select {col}
          </option>
          {dynamicOptions[col]?.map((option: string) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      );
    } else {
      return (
        <input
          type="text"
          name={col}
          value={formData[col] || ''}
          onChange={handleChange}
          className="bg-black text-yellow-100/50 px-3 py-2 rounded-lg border border-yellow-100/30 w-full text-sm"
          disabled={!isFieldEnabled(col)}
        />
      );
    }
  };

  if (!config) {
    return <div>Loading...</div>;
  }

  const filteredColumns = config.scopes.create ? columns.filter(column => !['id', 'created_at', 'updated_at', 'user_id'].includes(column)) : [];

  return (
    <div className="fixed inset-0 flex items-center justify-center bg-black bg-opacity-50 z-50 py-4">
      <div className="bg-black border border-yellow-100/30 p-6 rounded-lg w-3/4 max-h-screen overflow-y-auto">
        <h2 className="text-yellow-100/50 text-center mb-8">Create New {modalName.charAt(0).toUpperCase() + modalName.slice(1)}</h2>
        <form onSubmit={handleSubmit}>
          <div className="grid grid-cols-2 gap-4">
            {filteredColumns.map((col) => (
              <div key={col} className="mb-2">
                <label className="block text-yellow-100/50 ms-1 text-sm">{col}</label>
                {renderField(col)}
                {errors[col] && <p className="text-red-500">{errors[col]}</p>}
              </div>
            ))}
          </div>
          <div className="flex justify-end mt-4">
            <button
              type="button"
              onClick={onClose}
              className="bg-black hover:bg-yellow-100/70 text-yellow-100/50 hover:text-black py-1 px-4 rounded-lg text-sm border border-yellow-100/30 hover:border-black mr-2"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="bg-black hover:bg-yellow-100/70 text-yellow-100/50 hover:text-black py-1 px-4 rounded-lg text-sm border border-yellow-100/30 hover:border-black"
            >
              Create
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default CreateModal;
