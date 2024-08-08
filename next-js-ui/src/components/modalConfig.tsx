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
  read_summary: string[];
  update: string[];
  delete: boolean;
}

interface ValidationRules {
  [key: string]: string[];
}

interface AIQualityChecks {
  [key: string]: string[];
}

interface ReadRouteConfig {
  belongs_to_user_id: boolean;
}

interface ModalConfig {
  options: Options;
  conditional_options?: {
    [key: string]: ConditionalOption[];
  };
  scopes: Scopes;
  validation_rules: ValidationRules;
  ai_quality_checks: AIQualityChecks;
  read_routes: {
    [key: string]: ReadRouteConfig;
  };
}

interface ModalConfigMap {
  [key: string]: ModalConfig;
}

const modalConfig: ModalConfigMap = {
  "users": {
    "options": {
      "type[XOR]": [
        "admin",
        "normal"
      ]
    },
    "conditional_options": {},
    "scopes": {
      "create": true,
      "read": [
        "id",
        "username",
        "password",
        "type",
        "created_at",
        "updated_at"
      ],
      "update": [
        "username",
        "password",
        "type"
      ],
      "delete": true
    },
    "validation_rules": {
      "username": [
        "REQUIRED"
      ],
      "password": [
        "REQUIRED"
      ]
    },
    "ai_quality_checks": {},
    "read_routes": {
      "default": {
        "belongs_to_user_id": false
      }
    }
  }
};

export default modalConfig;



