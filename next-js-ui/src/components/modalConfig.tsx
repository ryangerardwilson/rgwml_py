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
      "read_summary": [
        "id",
        "username",
        "password",
        "type"
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
  },
  "router_recovery_outgoing_calls": {
    "options": {
      "customer_primary_remarks[XOR]": [
        "ready_to_return",
        "recharge_and_resume",
        "migration_or_shifting",
        "dispute_or_refund",
        "service_issue",
        "already_returned",
        "out_of_town",
        "other"
      ]
    },
    "conditional_options": {
      "customer_secondary_remarks": [
        {
          "condition": "customer_primary_remarks == ready_to_return",
          "options": [
            "disconnect_and_handover",
            "cannot_disconnect_and_handover"
          ]
        },
        {
          "condition": "customer_primary_remarks == recharge_and_resume",
          "options": [
            "follow_up_in_24_hours",
            "assign_to_ra"
          ]
        },
        {
          "condition": "customer_primary_remarks == migration_or_shifting",
          "options": [
            "shifting_ticket_created",
            "shifting_approved",
            "shifting_denied",
            "shifting_ticket_no_update"
          ]
        },
        {
          "condition": "customer_primary_remarks == dispute_or_refund",
          "options": [
            "refund_request_created",
            "refund_approved",
            "refund_denied",
            "refund_request_no_update"
          ]
        },
        {
          "condition": "customer_primary_remarks == already_returned",
          "options": [
            "returned_to_lco",
            "returned_to_wiom_official",
            "pata_nahi_kisko_de_diya"
          ]
        },
        {
          "condition": "customer_primary_remarks == service_issue",
          "options": [
            "refund_needed",
            "esclataed_to_nqt",
            "other"
          ]
        },
        {
          "condition": "customer_primary_remarks == out_of_town",
          "options": [
            "not_applicable"
          ]
        },
        {
          "condition": "customer_primary_remarks == other",
          "options": [
            "other"
          ]
        }
      ]
    },
    "scopes": {
      "create": false,
      "read": [
        "id",
        "user_id",
        "belongs_to_user_id",
        "mobile",
        "source",
        "device_id",
        "plan_expiry_date",
        "customer_name",
        "customer_address",
        "zone",
        "lco_contact",
        "pin_code",
        "latitude",
        "longitude",
        "am_name",
        "customer_primary_remarks",
        "customer_secondary_remarks",
        "follow_up_date",
        "comment",
        "created_at",
        "updated_at"
      ],
      "read_summary": [
        "mobile",
        "device_id",
        "customer_name"
      ],
      "update": [
        "category",
        "customer_primary_remarks",
        "customer_secondary_remarks",
        "follow_up_date",
        "comment"
      ],
      "delete": false
    },
    "validation_rules": {
      "customer_primary_remarks": [
        "REQUIRED"
      ],
      "customer_secondary_remarks": [
        "REQUIRED"
      ],
      "follow_up_date": [
        "IS_AFTER_TODAY"
      ]
    },
    "ai_quality_checks": {},
    "read_routes": {
      "uninitiated": {
        "belongs_to_user_id": true
      },
      "initiated": {
        "belongs_to_user_id": true
      },
      "follow-up-due-today": {
        "belongs_to_user_id": true
      },
      "follow-up-overdue": {
        "belongs_to_user_id": true
      }
    }
  }
};

export default modalConfig;