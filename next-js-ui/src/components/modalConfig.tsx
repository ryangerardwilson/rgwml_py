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
  validation_rules: ValidationRules;
  ai_quality_checks: AIQualityChecks;
  read_routes: string[];
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
    "read_routes": [
      "default"
    ]
  },
  "social_media_escalations": {
    "options": {
      "star_rating[XOR]": [
        "1",
        "2",
        "3",
        "4",
        "5",
        "NA"
      ],
      "forum[XOR]": [
        "playstore",
        "google_review",
        "freshdesk",
        "mail_to_management",
        "facebook",
        "instagram",
        "twitter",
        "linkedin",
        "other"
      ],
      "reach_out_method[XOR]": [
        "post",
        "comment",
        "dm",
        "mail",
        "other"
      ],
      "action_taken[XOR]": [
        "no_customer_detail_found",
        "resolved",
        "post_removed_and_resolved",
        "pending_from_customer",
        "escalated_to_nqt",
        "escalated_to_cops",
        "other"
      ]
    },
    "conditional_options": {},
    "scopes": {
      "create": true,
      "read": [
        "id",
        "user_id",
        "url",
        "post_text",
        "post_author",
        "star_rating",
        "forum",
        "mobile",
        "ai_sentiment",
        "ai_category",
        "ai_author_type",
        "reach_out_method",
        "action_taken",
        "comment",
        "follow_up_date",
        "created_at",
        "updated_at"
      ],
      "update": [
        "url",
        "post_text",
        "post_author",
        "star_rating",
        "forum",
        "mobile",
        "reach_out_method",
        "action_taken",
        "comment",
        "follow_up_date"
      ],
      "delete": true
    },
    "validation_rules": {
      "post_text": [
        "REQUIRED"
      ],
      "post_author": [
        "REQUIRED"
      ],
      "forum": [
        "REQUIRED"
      ],
      "reach_out_method": [
        "REQUIRED"
      ],
      "action_taken": [
        "REQUIRED"
      ],
      "mobile": [
        "IS_INDIAN_MOBILE_NUMBER"
      ],
      "follow_up_date": [
        "IS_AFTER_TODAY"
      ]
    },
    "ai_quality_checks": {},
    "read_routes": [
      "a-most-recent-100",
      "b-follow-up-due-today",
      "c-follow-up-overdue"
    ]
  },
  "welcome_calls": {
    "options": {
      "disposition[XOR]": [
        "wc_completed",
        "dnp",
        "asked_to_call_back",
        "call_disconnected_in_between"
      ],
      "issue[XOR]": [
        "no_issue",
        "internet_issue",
        "misbehave",
        "not_proper_install",
        "other_issue",
        "NA"
      ]
    },
    "conditional_options": {
      "sub_issue": [
        {
          "condition": "issue == no_issue",
          "options": [
            "happy",
            "neutral",
            "unhappy"
          ]
        },
        {
          "condition": "issue == internet_issue",
          "options": [
            "slow_speed",
            "range",
            "frequent_disconnect",
            "internet_down",
            "slow_speed_and_frequent_disconnect",
            "other"
          ]
        },
        {
          "condition": "issue == misbehave",
          "options": [
            "rude_behaviour",
            "fake_installation",
            "disintermediation",
            "false_promises",
            "took_extra_cash",
            "demanded_extra_cash",
            "other"
          ]
        },
        {
          "condition": "issue == not_proper_install",
          "options": [
            "untidy_wiring",
            "wrong_positioning",
            "other"
          ]
        },
        {
          "condition": "issue == other_issue",
          "options": [
            "other_issue"
          ]
        },
        {
          "condition": "issue == NA",
          "options": [
            "NA"
          ]
        }
      ]
    },
    "scopes": {
      "create": false,
      "read": [
        "id",
        "user_id",
        "mobile",
        "name",
        "city",
        "device_id",
        "plan_amount",
        "payment_mode",
        "device_type",
        "data_usage_rng",
        "priority",
        "disposition",
        "issue",
        "sub_issue",
        "alternate_number",
        "call_back_date",
        "comment",
        "created_at",
        "updated_at"
      ],
      "update": [
        "disposition",
        "issue",
        "sub_issue",
        "alternate_number",
        "call_back_date",
        "comment"
      ],
      "delete": false
    },
    "validation_rules": {
      "disposition": [
        "REQUIRED"
      ],
      "issue": [
        "REQUIRED"
      ],
      "sub_issue": [
        "REQUIRED"
      ],
      "alternate_number": [
        "IS_INDIAN_MOBILE_NUMBER"
      ],
      "call_back_date": [
        "IS_AFTER_TODAY"
      ],
      "comment": [
        "REQUIRED"
      ]
    },
    "ai_quality_checks": {
      "comment": [
        "the text should not be gibberish"
      ]
    },
    "read_routes": [
      "a-unprosecuted",
      "b-prosecuted",
      "c-call-back-due-today",
      "d-call-back-overdue"
    ]
  },
  "call_audits": {
    "options": {
      "disposition[XOR]": [
        "all_parameters_met",
        "fatal",
        "feedback",
        "warning",
        "zero_tolerance_policy"
      ],
      "sub_disposition[OR]": [
        "tat",
        "no_proper_rebuttals",
        "opening_in_5_secs",
        "closing",
        "hold_procedure_or_transfer_procedure",
        "further_assistance",
        "assurance_to_cx",
        "personalization",
        "giving_incomplete_and_incorrect_info_to_customer",
        "wrong_on_call_resolution_or_not_done_but_needed",
        "right_choice_of_words",
        "tonality_or_rate_of_speech",
        "not_attentive",
        "wrong_tagging_in_crm",
        "sarcasm",
        "rudeness",
        "shouting",
        "hanging_up_before_resolving_customer_issue",
        "providing_third_party_information"
      ]
    },
    "conditional_options": {},
    "scopes": {
      "create": false,
      "read": [
        "id",
        "user_id",
        "tse_id",
        "call_id",
        "end_stamp",
        "client_number",
        "recording_url",
        "agent_name",
        "all_agent_name",
        "agent_queue_name",
        "agent_department_name",
        "agent_campaign_name",
        "actual_speak_time",
        "raw_call_flow",
        "raw_transcriptions",
        "transcriptions_flow",
        "transcriptions_summary",
        "transcriptions_flow_anger",
        "transcriptions_flow_fear",
        "transcriptions_flow_joy",
        "transcriptions_flow_love",
        "transcriptions_flow_sadness",
        "transcriptions_flow_surprise",
        "transcriptions_model",
        "classifications_model",
        "disposition",
        "sub_disposition",
        "comment"
      ],
      "update": [
        "disposition",
        "sub_disposition",
        "comment"
      ],
      "delete": false
    },
    "validation_rules": {
      "disposition": [
        "REQUIRED"
      ],
      "sub_disposition": [
        "REQUIRED"
      ],
      "comment": [
        "REQUIRED"
      ]
    },
    "ai_quality_checks": {
      "comment": [
        "the text should not be gibberish"
      ]
    },
    "read_routes": [
      "a-most-recent-100-undisposed",
      "b-most-recent-100-high-pain-undisposed",
      "c-most-recent-100-disposed",
      "d-most-recent-100-welcome-calls",
      "e-most-recent-100-retention-calls",
      "f-most-recent-100-lead-status-calls",
      "g-most-recent-100-lead-sales-calls",
      "h-most-recent-100-ai-audited-long-calls"
    ]
  }
};

export default modalConfig;



