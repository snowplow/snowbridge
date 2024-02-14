// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.

/* amplitudeConfig represents the user input that configures this transformation
 *
 * apiKey: non empty string
 * includeSelfDescribingEvent: boolean
 * extractFromArray: boolean
 * includeEntities: one-of 'all','none'
 * entityMappingRules: Array
 *   with elements: Objects with props:
 *     key: non-empty string
 *     mappedKey: string
 *     propertiesObjectToPopulate: one-of 'event_properties','user_properties'
 *     version: one-of 'control','free'
 * entityExclusionRules: Array
 *   with elements: Objects with props:
 *     key: non-empty string
 *     version: one-of 'control','free'
 * includeCommonEventProperties: boolean
 * entityMappingRules: Array
 *   with elements: Objects with props
 *     key: non-empty string
 *     mappedKey: string
 * includeCommpnUserProperties: boolean
 * userMappingRules: Array
 *   with elements: Objects with props
 *     key: non-empty string
 *     mappedKey: string
 * forwardIp: boolean
 * amplitudeTime: one-of 'no','current','eventProperty'
 * timeProp: non-empty string
 */
const amplitudeConfig = {
  apiKey: '12345',
  includeSelfDescribingEvent: false,
  extractFromArray: true,
  includeEntities: 'all',
  entityMappingRules: [
    {
      key: 'iglu:com.youtube/youtube/jsonschema/1-0-0',
      mappedKey: 'youtube',
      propertiesObjectToPopulate: 'event_properties',
      version: 'free',
    },
    {
      key: 'contexts_com_snowplowanalytics_snowplow_media_player',
      mappedKey: 'media_player',
      propertiesObjectToPopulate: 'event_properties',
      version: 'free',
    },
    {
      key: 'contexts_com_google_tag-manager_server-side_user_data_1',
      mappedKey: 'user_data',
      propertiesObjectToPopulate: 'user_properties',
      version: 'control',
    },
  ],
  entityExclusionRules: [
    {
      key: 'contexts_com_snowplowanalytics_snowplow_web_page_5',
      version: 'free',
    },
    {
      key: 'iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-2',
      version: 'control',
    },
  ],
  includeCommonEventProperties: true,
  eventMappingRules: [
    {
      key: 'unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1.type',
      mappedKey: 'media_event_type',
    },
    {
      key: 'name_tracker',
      mappedKey: 'tracker',
    },
  ],
  includeCommonUserProperties: true,
  mktToUserUtm: false,
  userMappingRules: [
    {
      key: 'contexts_com_google_tag-manager_server-side_user_data_1.0.email_address',
      mappedKey: 'email',
    },
  ],
  forwardIp: false,
  amplitudeTime: 'eventProperty',
  timeProp: 'collector_tstamp',
};

// Constants
const SGTM_USER_DATA =
  'contexts_com_google_tag-manager_server-side_user_data_1';
const CLIENT_SESSION =
  'contexts_com_snowplowanalytics_snowplow_client_session_1';
const MOBILE_CONTEXT =
  'contexts_com_snowplowanalytics_snowplow_mobile_context_1';
const YAUAA_CONTEXT = 'contexts_nl_basjes_yauaa_context_1';

const SP_ATOMIC_TSTAMPS = [
  'collector_tstamp',
  'derived_tstamp',
  'true_tstamp',
  'dvce_created_tstamp',
  'dvce_sent_tstamp',
  'etl_tstamp',
  'refr_dvce_tstamp',
];
const MKT_TO_UTM_MAP = [
  { key: 'mkt_source', mappedKey: 'utm_source' },
  { key: 'mkt_medium', mappedKey: 'utm_medium' },
  { key: 'mkt_campaign', mappedKey: 'utm_campaign' },
  { key: 'mkt_term', mappedKey: 'utm_term' },
  { key: 'mkt_content', mappedKey: 'utm_content' },
];

// Helpers
const isValidRule = (spec) => {
  return function (x) {
    if (Object.prototype.toString.call(x) !== '[object Object]') {
      return false;
    }

    if (!x.key) {
      return false;
    }

    const props = ['event_properties', 'user_properties'];
    const version = ['control', 'free'];
    switch (spec) {
      case 'inclusion':
        if (props.indexOf(x.propertiesObjectToPopulate) < 0) {
          return false;
        }

        if (version.indexOf(x.version) < 0) {
          return false;
        }

        return true;
      case 'exclusion':
        if (version.indexOf(x.version) < 0) {
          return false;
        }

        return true;
      case 'common':
        return true;
      default:
        return false;
    }
  };
};

const validate = (rules, spec) => {
  return rules.map(isValidRule(spec)).reduce((acc, curr) => {
    return acc && curr;
  });
};

/*
 * This function is meant to validate the amplitude configuration object.
 * It exists to guarantee parity with the gtm-ss amplitude tag config,
 * in order to guard on any assumptions.
 * This is not an exhaustive check.
 */
const isValidConfig = (tagConfig) => {
  if (!tagConfig.apiKey) {
    return false;
  }

  const allowedIncludeEntities = ['all', 'none'];
  if (allowedIncludeEntities.indexOf(tagConfig.includeEntities) < 0) {
    return false;
  }

  const entityMappingRules = tagConfig.entityMappingRules;
  if (entityMappingRules && entityMappingRules.length > 0) {
    if (!validate(entityMappingRules, 'inclusion')) {
      return false;
    }
  }

  const entityExclusionRules = tagConfig.entityExclusionRules;
  if (entityExclusionRules && entityExclusionRules.length > 0) {
    if (!validate(entityExclusionRules, 'exclusion')) {
      return false;
    }
  }

  const eventMappingRules = tagConfig.eventMappingRules;
  if (eventMappingRules && eventMappingRules.length > 0) {
    if (!validate(eventMappingRules, 'common')) {
      return false;
    }
  }

  const userMappingRules = tagConfig.userMappingRules;
  if (userMappingRules && userMappingRules.length > 0) {
    if (!validate(userMappingRules, 'common')) {
      return false;
    }
  }

  const allowedAmplitudeTime = ['no', 'current', 'eventProperty'];
  const amplitudeTime = tagConfig.amplitudeTime;
  if (allowedAmplitudeTime.indexOf(amplitudeTime) < 0) {
    return false;
  }
  if (amplitudeTime === 'eventProperty' && !tagConfig.timeProp) {
    return false;
  }

  return true;
};

const getAllEventData = (engineProtocol) => {
  return engineProtocol['Data'];
};

const getTimestampMillis = () => {
  return new Date().getTime();
};

/*
 * Gets the value in obj from path.
 * Path must be a string denoting a (nested) property path separated by '.'
 *  e.g. getFromPath('a.b', {a: {b: 2}}) => 2
 *
 * @param path {string} - the string to replace into
 * @param obj {Object} - the object to look into
 * @returns - the corresponding value or undefined
 */
const getFromPath = (path, obj) => {
  if (typeof path === 'string') {
    const splitPath = path.split('.').filter((p) => !!p);
    return splitPath.reduce((acc, curr) => acc && acc[curr], obj);
  }
  return undefined;
};

/*
 * Given width W and height H, returns 'WxH'.
 * Performs string concatenation, so assumes the types of its arguments are
 * strings, numbers or booleans.
 */
const mkDims = (width, height) => {
  if (width && height) {
    return width + 'x' + height;
  }
  return undefined;
};

/*
 * Determines if a property name corresponds to a Snowplow enriched timestamp.
 *
 * @returns - boolean
 */
const isSpTstampProp = (propName) => {
  if (SP_ATOMIC_TSTAMPS.indexOf(propName) >= 0) {
    return true;
  }
  return false;
};

/*
 * Determines whether the event is a snowplow enriched event.
 * Since we assume snowplow_mode=true, just returns true.
 * Could be removed, but stays for reference with gtm-ss
 *
 * @returns - boolean
 */
const isSpEnrichedEvent = () => {
  return true;
};

/*
 * Converts an iso time/timestamp to unix milliseconds.
 * This function is being used here to convert an iso date to unix millis:
 * 1. for an atomic timestamp property:
 *    In these cases the analytics sdk ToMap() returns them as time.Time.
 *    This means they need special handling inside JS.
 *    https://pkg.go.dev/github.com/dop251/goja#hdr-Handling_of_time_Time
 *    It is safe to use UnixNano (i.e. ignore timezone info), since the
 *    snowplow atomic timestamps are in UTC.
 * 2. for the firstEventTimestamp of the client_session context
 *    This is a string and not time.Time by the analytics sdk.
 *
 * @param isoTime {string | Object} - the ISO timestamp or time to convert
 * @param handleTime {bool} - whether time handling is needed (see above)
 * @returns {number} - Integer number representing Unix milliseconds
 */
function isoToUnixMillis(isoTime, handleTime) {
  if (handleTime) {
    return new Date(isoTime.UnixNano() / 1e6).getTime();
  }

  return new Date(isoTime).getTime();
}

const cleanObject = (obj) => {
  let target = {};

  for (let prop in obj) {
    if (obj.hasOwnProperty(prop) && obj[prop] != null) {
      target[prop] = obj[prop];
    }
  }

  return target;
};

const merge = (args) => {
  let target = {};

  const addToTarget = (obj) => {
    for (let prop in obj) {
      if (obj.hasOwnProperty(prop)) {
        target[prop] = obj[prop];
      }
    }
  };

  for (let i = 0; i < args.length; i++) {
    addToTarget(args[i]);
  }

  return target;
};

const getEventDataByKeys = (configProps, evData) => {
  const props = {};
  configProps.forEach((p) => {
    let eventProperty = getFromPath(p.key, evData);
    if (eventProperty) {
      props[p.mappedKey || p.key] = eventProperty;
    }
  });
  return props;
};

const replaceAll = (str, substr, newSubstr) => {
  let finished = false,
    result = str;
  while (!finished) {
    const newStr = result.replace(substr, newSubstr);
    if (result === newStr) {
      finished = true;
    }
    result = newStr;
  }
  return result;
};

const isUpper = (value) => {
  return value === value.toUpperCase() && value !== value.toLowerCase();
};

const toSnakeCase = (value) => {
  let result = '';
  let previousChar;
  for (var i = 0; i < value.length; i++) {
    let currentChar = value.charAt(i);
    if (isUpper(currentChar) && i > 0 && previousChar !== '_') {
      result = result + '_' + currentChar;
    } else {
      result = result + currentChar;
    }
    previousChar = currentChar;
  }
  return result;
};

const extractFromArrayIfSingleElement = (arr, tagConfig) =>
  arr.length === 1 && tagConfig.extractFromArray ? arr[0] : arr;

/*
 * Parses a Snowplow schema to the expected major version format,
 *  also prefixed so as to match the contexts' output of the Snowplow Client.
 *
 * @param schema {string} - the input schema
 * @returns - the expected output client event property
 */
const parseSchemaToMajorKeyValue = (schema) => {
  if (schema.indexOf('contexts_') === 0) return schema;
  if (schema.indexOf('iglu:') === 0) {
    let fixed = replaceAll(
      replaceAll(
        schema.replace('iglu:', '').replace('jsonschema/', ''),
        '.',
        '_'
      ),
      '/',
      '_'
    );

    for (let i = 0; i < 2; i++) {
      fixed = fixed.substring(0, fixed.lastIndexOf('-'));
    }
    return 'contexts_' + toSnakeCase(fixed).toLowerCase();
  }
  return schema;
};

/*
 * Returns whether a property name is a Snowplow self-describing event property.
 */
const isSpSelfDescProp = (prop) => {
  return prop.indexOf('unstruct_event_') === 0;
};

/*
 * Returns whether a property name is a Snowplow context/entity property.
 */
const isSpContextsProp = (prop) => {
  return prop.indexOf('contexts_') === 0;
};

/*
 * Given a list of entity references and an entity name,
 * returns the index of a matching reference.
 * Matching reference means whether the entity name starts with ref.
 *
 * @param entity {string} - the entity name to match
 * @param refsList {Array} - an array of strings
 */
const getReferenceIdx = (entity, refsList) => {
  for (let i = 0; i < refsList.length; i++) {
    if (entity.indexOf(refsList[i]) === 0) {
      return i;
    }
  }
  return -1;
};

/*
 * Filters out invalid rules to avoid unintended behavior.
 * (e.g. version control being ignored if version num is not included in name)
 * Assumes that a rule contains 'key' and 'version' properties.
 */
const cleanRules = (rules) => {
  return rules.filter((row) => {
    if (row.version === 'control') {
      const lastCharAsNum = parseInt(row.key.slice(-1));
      if (!lastCharAsNum && lastCharAsNum !== 0) {
        // was not a digit, so invalid rule
        return false;
      }
      return true;
    }
    return true;
  });
};

/*
 * Parses the entity exclusion rules from the tag configuration.
 */
const parseEntityExclusionRules = (tagConfig) => {
  const rules = tagConfig.entityExclusionRules;
  if (rules) {
    const validRules = cleanRules(rules);
    const excludedEntities = validRules.map((row) => {
      const entityRef = parseSchemaToMajorKeyValue(row.key);
      const versionFreeRef = entityRef.slice(0, -2);
      return {
        ref: row.version === 'control' ? entityRef : versionFreeRef,
        version: row.version,
      };
    });
    return excludedEntities;
  }
  return [];
};

/*
 * Parses the entity inclusion rules from the tag configuration.
 */
const parseEntityRules = (tagConfig) => {
  const rules = tagConfig.entityMappingRules;
  if (rules) {
    const validRules = cleanRules(rules);
    const parsedRules = validRules.map((row) => {
      const parsedKey = parseSchemaToMajorKeyValue(row.key);
      const versionFreeKey = parsedKey.slice(0, -2);
      return {
        ref: row.version === 'control' ? parsedKey : versionFreeKey,
        parsedKey: parsedKey,
        mappedKey: row.mappedKey || parsedKey,
        target: row.propertiesObjectToPopulate,
        version: row.version,
      };
    });
    return parsedRules;
  }
  return [];
};

/*
 * Given the inclusion rules and the excluded entity references,
 * returns the final entity mapping rules.
 */
const finalizeEntityRules = (inclusionRules, excludedRefs) => {
  const finalEntities = inclusionRules.filter((row) => {
    const refIdx = getReferenceIdx(row.ref, excludedRefs);
    return refIdx < 0;
  });
  return finalEntities;
};

const cleanPropertyName = (prop) => {
  return prop.replace('unstruct_event', 'self_describing_event');
};

const parseCustomEventAndEntities = (
  evData,
  tagConfig,
  eventProperties,
  userProperties
) => {
  const inclusionRules = parseEntityRules(tagConfig);
  const exclusionRules = parseEntityExclusionRules(tagConfig);
  const excludedRefs = exclusionRules.map((r) => r.ref);
  const finalEntityRules = finalizeEntityRules(inclusionRules, excludedRefs);
  const finalEntityRefs = finalEntityRules.map((r) => r.ref);

  for (let prop in evData) {
    if (evData.hasOwnProperty(prop)) {
      if (isSpSelfDescProp(prop) && tagConfig.includeSelfDescribingEvent) {
        eventProperties[cleanPropertyName(prop)] = evData[prop];
        continue;
      }

      if (isSpContextsProp(prop)) {
        if (getReferenceIdx(prop, excludedRefs) >= 0) {
          continue;
        }
        const ctxVal = extractFromArrayIfSingleElement(evData[prop], tagConfig);
        const refIdx = getReferenceIdx(prop, finalEntityRefs);
        if (refIdx >= 0) {
          const rule = finalEntityRules[refIdx];
          const target =
            rule.target === 'event_properties'
              ? eventProperties
              : userProperties;
          target[rule.mappedKey] = ctxVal;
        } else {
          if (tagConfig.includeEntities === 'none') {
            continue;
          }

          if (getReferenceIdx(prop, excludedRefs) < 0) {
            eventProperties[prop] = ctxVal;
          }
        }
      }
    }
  }
};

/*
 * Initializes the user_properties of the Ampitude event
 * based on the User Property Rules of the tag configuration.
 *
 * @param evData {Object} - the client event object
 * @param tagConfig {Object} - the tag configuration
 * @returns - Object
 */
const initUserData = (evData, tagConfig) => {
  // include common user properties from gtm-ss user_data entity
  const sgtmUserData = evData[SGTM_USER_DATA];
  const includeCommon = !!(
    tagConfig.includeCommonUserProperties && sgtmUserData
  );
  const commonUserData = includeCommon ? sgtmUserData[0] : {};

  // map Snowplow mkt fields
  const utmData = tagConfig.mktToUserUtm ? MKT_TO_UTM_MAP : [];

  // additional user property mapping rules
  const tagUserMapRules = tagConfig.userMappingRules;
  const includeCustom = !!(tagUserMapRules && tagUserMapRules.length > 0);
  const userMappingRules = includeCustom ? tagUserMapRules : [];

  // additional rules take precedence
  const additionalUserProps = utmData.concat(userMappingRules);

  return merge([
    commonUserData,
    getEventDataByKeys(additionalUserProps, evData),
  ]);
};

/*
 * Initializes the event_properties of the Ampitude event.
 *
 * @param evData {Object} - the client event object
 * @param tagConfig {Object} - the tag configuration
 * @returns - Object
 */
const initEventProperties = (evData, tagConfig) => {
  let eventProps = {};

  if (tagConfig.includeCommonEventProperties) {
    const screenRes = mkDims(evData.dvce_screenwidth, evData.dvce_screenheight);
    const viewpSize = mkDims(evData.br_viewwidth, evData.br_viewheight);

    eventProps.page_location = evData.page_url;
    eventProps.page_encoding = evData.doc_charset;
    eventProps.page_referrer = evData.page_referrer;
    eventProps.page_title = evData.page_title;
    eventProps.screen_resolution = screenRes;
    eventProps.viewport_size = viewpSize;
  }

  const eventMappingRules = tagConfig.eventMappingRules;
  if (eventMappingRules && eventMappingRules.length > 0) {
    eventProps = merge([
      eventProps,
      getEventDataByKeys(eventMappingRules, evData),
    ]);
  }

  return eventProps;
};

/*
 * Returns the time property for Amplitude event
 * depending on time settings configured.
 *
 * @param evData {Object} - the client event object
 * @param tagConfig {Object} - the tag configuration object
 * @returns - unix timestamp or undefined
 */
const getAmplitudeTime = (evData, tagConfig) => {
  const timeSetting = tagConfig.amplitudeTime;
  switch (timeSetting) {
    case 'no':
      return undefined;
    case 'current':
      return getTimestampMillis();
    case 'eventProperty':
      const timeProp = tagConfig.timeProp;
      const timeValue = getFromPath(timeProp, evData);
      if (isSpTstampProp(timeProp) && isSpEnrichedEvent()) {
        return isoToUnixMillis(timeValue, true);
      }
      // with extra check to ensure null is not NaN
      const numValue = parseInt(timeValue);
      if (isNaN(numValue)) {
        return undefined;
      }
      return numValue;
    default:
      // default as 'no'
      return undefined;
  }
};

/*
 * Returns the session_id (long - unix timestamp) for Amplitude event
 * from the firstEventTimestamp of the client_session context.
 *
 * @param evData {Object} - the client event object
 * @returns - unix timestamp or undefined
 */
const getAmplitudeSession = (evData) => {
  const clientSessionCtx = evData[CLIENT_SESSION];
  if (clientSessionCtx) {
    const firstEventTime = clientSessionCtx[0].firstEventTimestamp;
    return isoToUnixMillis(firstEventTime, false);
  }
  return undefined;
};

/*
 * Contructs an Amplitude event.
 */
const mkAmplitudeEvent = (evData, tagConfig, eventProps, userProps) => {
  // since we are in snowplow_mode we know that:
  // 'event_id' and 'platform' exist - no need for fallbacks
  let insertId = evData.event_id;
  let platform = evData.platform;

  let amplitudeEvent = {
    event_type: evData.event_name,
    device_id: evData.domain_userid,
    ip: amplitudeConfig.forwardIp ? evData.user_ipaddress : undefined,
    time: getAmplitudeTime(evData, amplitudeConfig),
    session_id: getAmplitudeSession(evData),
    event_properties: cleanObject(eventProps),
    user_properties: cleanObject(userProps),
    platform: platform,
    country: evData.geo_country,
    region: evData.geo_region,
    city: evData.geo_city,
    location_lat: evData.geo_latitude,
    location_lng: evData.geo_longitude,
    carrier: evData.ip_organization,
    language: evData.br_lang,
    insert_id: insertId,
    user_id: evData.user_id,
  };

  const yauaa = evData[YAUAA_CONTEXT];
  if (yauaa) {
    const yauaaContext = yauaa[0];
    amplitudeEvent.os_name = yauaaContext.operatingSystemName;
    amplitudeEvent.os_version = yauaaContext.operatingSystemVersion;
    amplitudeEvent.device_brand = yauaaContext.deviceBrand;
    amplitudeEvent.device_model = yauaaContext.deviceName;
  }

  const mob = evData[MOBILE_CONTEXT];
  if (mob) {
    const mobContext = mob[0];
    amplitudeEvent.os_name = mobContext.osType;
    amplitudeEvent.os_version = mobContext.osVersion;
    amplitudeEvent.device_manufacturer = mobContext.deviceManufacturer;
    amplitudeEvent.device_model = mobContext.deviceModel;
    amplitudeEvent.carrier = mobContext.carrier;
    amplitudeEvent.idfa = mobContext.appleIdfa;
    amplitudeEvent.idfv = mobContext.appleIdfv;
    amplitudeEvent.adid = mobContext.androidIdfa;
  }

  return cleanObject(amplitudeEvent);
};

// Main - Assumes snowplow_mode=true
function main(input) {
  // validate amplitude configuration object
  if (!isValidConfig(amplitudeConfig)) {
    throw new Error('invalid amplitude configuration provided');
  }

  // get data from engine protocol
  const eventData = getAllEventData(input);

  // construct user_properties and event_properties of the Amplitude event
  let userProperties = initUserData(eventData, amplitudeConfig);
  let eventProperties = initEventProperties(eventData, amplitudeConfig);
  parseCustomEventAndEntities(
    eventData,
    amplitudeConfig,
    eventProperties,
    userProperties
  );

  // make the Amplitude payload
  const amplitudeEvent = mkAmplitudeEvent(
    eventData,
    amplitudeConfig,
    eventProperties,
    userProperties
  );

  const authedAmplitudeBody = {
    api_key: amplitudeConfig.apiKey,
    events: [amplitudeEvent],
  };

  // we have 2 options here
  // 1. set Data (i.e. mutate input) and return input
  // input.Data = authedAmplitudeBody;
  // return input;
  //
  // 2. return a new engine protocol
  return {
    Data: authedAmplitudeBody,
  };
}
