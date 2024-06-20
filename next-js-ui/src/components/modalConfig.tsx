// src/components/modalConfig.tsx

interface Options {
  [key: string]: string[] | undefined; // Allows any string key with string array or undefined value
}

interface ConditionalOption {
  condition: string;
  options: string[];
}

interface Scopes {
  create: boolean;
  read: string[];
  update: string[];
  delete: boolean;
}

interface ValidationRules {
  [key: string]: string[];
}

interface AIQualityChecks {
  [key: string]: string[];
}

interface ModalConfig {
  options: Options;
  conditional_options?: {
    [key: string]: ConditionalOption[];
  };
  scopes: Scopes;
  validation_rules?: ValidationRules;
  ai_quality_checks?: AIQualityChecks;
}

interface ModalConfigMap {
  [key: string]: ModalConfig;
}

const modalConfig: ModalConfigMap = {
  users: {
    options: {
      type: ["admin", "normal"]
    },
    conditional_options: {},
    scopes: {
      create: true,
      read: ["id","username","password","type", "created_at","updated_at"],
      update: ["username","password","type"],
      delete: true
    },
    validation_rules: {
      "username": ["REQUIRED"],
      "password": ["REQUIRED"] 
    }
  },
  customers: {
    options: {
      issue: ["A", "B", "C"]
    },
    conditional_options: {
      status: [
	{
	   condition: "issue == A",
	   options: ["X1","X2","X3"]
	},
        {
           condition: "issue == B",
           options: ["Y1","Y2","Y3"]
        },
        {
           condition: "issue == C",
           options: ["Z1","Z2","Z3"]
        }      
      ] 
    },
    scopes: {
      create: true,
      read: ["id", "mobile", "issue", "status", "created_at"],
      update: ["mobile","issue", "status"],
      delete: true
    },
    validation_rules: {
      "mobile": ["REQUIRED"]
    },
    ai_quality_checks: {
      "mobile": ["rhymes with potato", "is a fruit or vegetable"]
    },
  },
  partners: {
    options: {
      issue: ["A", "B", "C"],
      status: ["X", "Y"]
    },
    scopes: {
      create: true,
      read: ["id", "mobile", "issue", "status", "created_at"],
      update: ["issue", "status"],
      delete: false
    }
  }
};

export default modalConfig;

