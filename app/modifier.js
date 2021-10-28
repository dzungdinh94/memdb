import utils from "./utils";

//http://docs.mongodb.org/manual/reference/limits/#Restrictions-on-Field-Names
const verifyDoc = function (doc) {
  if (doc === null || typeof doc !== "object") {
    return;
  }

  for (const key in doc) {
    if (key[0] === "$") {
      throw new Error('Document fields can not start with "$"');
    }
    if (key.indexOf(".") !== -1) {
      throw new Error('Document fields can not contain "."');
    }
    verifyDoc(doc[key]);
  }
};

export const $insert = function (doc, param) {
  param = param || {};
  if (doc !== null) {
    throw new Error("doc already exists");
  }
  verifyDoc(param);
  return param;
};

export const $replace = function (doc, param) {
  param = param || {};
  if (doc === null) {
    throw new Error("doc not exist");
  }
  verifyDoc(param);
  return param;
};

export const $remove = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  return null;
};

export const $set = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    verifyDoc(param[path]);
    utils.setObjPath(doc, path, param[path]);
  }
  return doc;
};

export const $unset = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    if (!!param[path]) {
      utils.deleteObjPath(doc, path);
    }
  }
  return doc;
};

export const $inc = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    const value = utils.getObjPath(doc, path);
    const delta = param[path];
    if (value === undefined) {
      value = 0;
    }
    if (typeof value !== "number" || typeof delta !== "number") {
      throw new Error("$inc non-number");
    }
    utils.setObjPath(doc, path, value + delta);
  }
  return doc;
};

export const $push = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    const arr = utils.getObjPath(doc, path);
    if (arr === undefined) {
      utils.setObjPath(doc, path, []);
      arr = utils.getObjPath(doc, path);
    }
    if (!Array.isArray(arr)) {
      throw new Error("$push to non-array");
    }
    verifyDoc(param[path]);
    arr.push(param[path]);
  }
  return doc;
};

export const $pushAll = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    const arr = utils.getObjPath(doc, path);
    if (arr === undefined) {
      utils.setObjPath(doc, path, []);
      arr = utils.getObjPath(doc, path);
    }
    if (!Array.isArray(arr)) {
      throw new Error("$push to non-array");
    }
    const items = param[path];
    if (!Array.isArray(items)) {
      items = [items];
    }
    for (const i in items) {
      verifyDoc(items[i]);
      arr.push(items[i]);
    }
  }
  return doc;
};

export const $addToSet = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    const arr = utils.getObjPath(doc, path);
    if (arr === undefined) {
      utils.setObjPath(doc, path, []);
      arr = utils.getObjPath(doc, path);
    }
    if (!Array.isArray(arr)) {
      throw new Error("$addToSet to non-array");
    }
    const value = param[path];
    if (arr.indexOf(value) === -1) {
      verifyDoc(value);
      arr.push(value);
    }
  }
  return doc;
};

export const $pop = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    const arr = utils.getObjPath(doc, path);
    if (Array.isArray(arr)) {
      arr.pop();
    }
  }
  return doc;
};

export const $pull = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    const arr = utils.getObjPath(doc, path);
    if (Array.isArray(arr)) {
      const value = param[path];
      const i = arr.indexOf(value);
      if (i !== -1) {
        arr.splice(i, 1);
      }
    }
  }
  return doc;
};

export const $pullAll = function (doc, param) {
  if (doc === null) {
    throw new Error("doc not exist");
  }
  for (const path in param) {
    const arr = utils.getObjPath(doc, path);
    if (Array.isArray(arr)) {
      const values = param[path];
      if (!Array.isArray(values)) {
        values = [values];
      }
      values.forEach(function (value) {
        const i = arr.indexOf(value);
        if (i !== -1) {
          arr.splice(i, 1);
        }
      }); //jshint ignore:line
    }
  }
  return doc;
};
