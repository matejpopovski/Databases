#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation, const int attrCnt, const attrInfo attrList[])
{
    // part 6 starts here
    Status status;
    Record record; // temporary record
    RID rid;
    int relationAttrCount;
    AttrDesc *attrDesc;
    int recordLen;
    int intVal;
    int floatVal;

    // Finding count of attributes in relation and retrieving attribute descriptors
    status = attrCat->getRelInfo(relation, relationAttrCount, attrDesc);

    if (status != OK)
    {
        return status; // Return status if operation fails
    }

    if (relationAttrCount != attrCnt)
    {
        // incorrect number of attributes provided
        return OK;
    }

    for (int i = 0; i < attrCnt; i++)
    {
        recordLen += attrDesc[i].attrLen; // Calculating total record length
    }

    InsertFileScan resultRel(relation, status);
    if (status != OK)
    {
        return status; // Return status if operation fails
    }
    // Allocating memory for record data
    record.length = recordLen;
    record.data = (char *)malloc(recordLen);
    // Iterating over attribute descriptors and attribute list
    for (int i = 0; i < relationAttrCount; i++)
    {
        for (int j = 0; j < attrCnt; j++)
        {
            // Matching attribute names
            if (strcmp(attrList[j].attrName, attrDesc[i].attrName) == 0)
            {
                // Handling different attribute types
                switch (attrList[j].attrType)
                {
                case FLOAT:
                    // Converting attribute value to float and copying to record data
                    floatVal = atof((char *)attrList[j].attrValue);
                    memcpy((char *)record.data + attrDesc[i].attrOffset, (char *)&floatVal, attrDesc[i].attrLen);
                    break;

                case INTEGER:
                    intVal = atoi((char *)attrList[j].attrValue);
                    memcpy((char *)record.data + attrDesc[i].attrOffset, (char *)&intVal, attrDesc[i].attrLen);
                    break;

                default:
                    // Copying attribute value to record data (for other types)
                    memcpy((char *)record.data + attrDesc[i].attrOffset, attrList[j].attrValue, attrDesc[i].attrLen);
                    break;
                }
                break;
            }
        }
    }
        // Inserting record into the relation
        resultRel.insertRecord(record, rid);

    return OK; // Returning OK status if reached
}
