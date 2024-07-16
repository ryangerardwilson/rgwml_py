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
  read_routes?: string[];
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
    "read_routes": [
      "default"
    ]
  },
  "social_media_escalations": {
    "options": {
      "forum[XOR]": [
        "Google_Reviews",
        "LinkedIn",
        "Twitter/X",
        "Facebook",
        "Instagram",
        "YouTube",
        "Other"
      ],
      "status[XOR]": [
        "Unresolved",
        "Resolved_but_post_not_removed",
        "Not_able_to_identify_poster"
      ],
      "issue[XOR]": [
        "Internet_supply_down",
        "Slow_speed",
        "Frequent_disconnect",
        "Rude_behaviour_of_Partner",
        "Booking_fee_refund",
        "Trust issue",
        "Other"
      ]
    },
    "conditional_options": {
      "sub_status": [
        {
          "condition": "status == Unresolved",
          "options": [
            "Did_not_pick_up",
            "Picked_up_yet_unresolved"
          ]
        },
        {
          "condition": "status == Resolved_but_post_not_removed",
          "options": [
            "Was_very_angry",
            "Other"
          ]
        }
      ]
    },
    "scopes": {
      "create": true,
      "read": [
        "id",
        "url",
        "forum",
        "mobile",
        "issue",
        "status",
        "sub_status",
        "action_taken",
        "follow_up_date",
        "created_at"
      ],
      "update": [
        "url",
        "forum",
        "mobile",
        "issue",
        "status",
        "sub_status",
        "action_taken",
        "follow_up_date"
      ],
      "delete": true
    },
    "validation_rules": {
      "url": [
        "REQUIRED"
      ],
      "forum": [
        "REQUIRED"
      ],
      "issue": [
        "REQUIRED"
      ],
      "status": [
        "REQUIRED"
      ],
      "sub_status": [
        "REQUIRED"
      ],
      "action_taken": [
        "REQUIRED"
      ],
      "follow_up_date": [
        "REQUIRED",
        "IS_AFTER_TODAY"
      ]
    },
    "ai_quality_checks": {
      "action_taken": [
        "must describe a meaningful step taken to reach out to a customer and resolve a social media escalation"
      ]
    },
    "read_routes": [
      "most-recent-500",
      "todays-cases",
      "yesterdays-cases"
    ]
  }
};

export default modalConfig;
