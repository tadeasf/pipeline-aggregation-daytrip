def pipeline_no_table():
    return [
        {"$sort": {"createdAt": 1}},
        {"$limit": 2000.0},
        {
            "$addFields": {
                "passengersCount": {"$size": "$passengers"},
                "vehiclesCount": {"$size": "$vehicles"},
                "additionalStopCount": {"$size": "$customLocations"},
            }
        },
        {"$unwind": {"path": "$discountIds", "preserveNullAndEmptyArrays": True}},
        {
            "$lookup": {
                "from": "discounts",
                "localField": "discountIds",
                "foreignField": "_id",
                "as": "discounts",
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "root": {"$first": "$$ROOT"},
                "discounts": {"$push": "$discounts"},
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {"$mergeObjects": ["$root", {"discounts": "$discounts"}]}
            }
        },
        {
            "$lookup": {
                "from": "assignations",
                "localField": "_id",
                "foreignField": "orderId",
                "as": "assignation",
            }
        },
        {
            "$lookup": {
                "from": "subsidies",
                "localField": "assignation.subsidyIds",
                "foreignField": "_id",
                "as": "subsidies",
            }
        },
        {
            "$lookup": {
                "from": "compensations",
                "localField": "assignation.compensationIds",
                "foreignField": "_id",
                "as": "compensations",
            }
        },
        {
            "$addFields": {
                "sumOfCompensations": {
                    "$reduce": {
                        "input": "$compensations",
                        "initialValue": 0.0,
                        "in": {"$sum": ["$$value", "$$this.value"]},
                    }
                }
            }
        },
        {
            "$lookup": {
                "from": "penalties",
                "localField": "assignation.penaltyIds",
                "foreignField": "_id",
                "as": "penalties",
            }
        },
        {
            "$addFields": {
                "sumOfPenalties": {
                    "$reduce": {
                        "input": "$penalties",
                        "initialValue": 0.0,
                        "in": {"$sum": ["$$value", "$$this.value"]},
                    }
                }
            }
        },
        {
            "$addFields": {
                "sumOfDiscountsFee": {
                    "$reduce": {
                        "input": {
                            "$map": {
                                "input": "$discounts",
                                "as": "nestedDiscountArray",
                                "in": {
                                    "$cond": [
                                        {"$ne": ["$$nestedDiscountArray", []]},
                                        {
                                            "$arrayElemAt": [
                                                "$$nestedDiscountArray.fee",
                                                0.0,
                                            ]
                                        },
                                        0.0,
                                    ]
                                },
                            }
                        },
                        "initialValue": 0.0,
                        "in": {"$add": ["$$value", "$$this"]},
                    }
                }
            }
        },
        {
            "$addFields": {
                "sumOfSubsidies": {
                    "$reduce": {
                        "input": "$subsidies",
                        "initialValue": 0.0,
                        "in": {"$sum": ["$$value", "$$this.value"]},
                    }
                }
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "assignation.userId",
                "foreignField": "_id",
                "as": "drivers",
            }
        },
        {
            "$lookup": {
                "from": "customerFeedbacks",
                "localField": "_id",
                "foreignField": "orderId",
                "as": "driverFeedback",
            }
        },
        {
            "$addFields": {
                "customLocationsCount": {"$size": "$customLocations"},
                "contentLocationsCount": {"$size": "$contentLocations"},
                "driverRating": {"$avg": "$driverFeedback.textScore"},
            }
        },
        {
            "$addFields": {
                "totalStopsCount": {
                    "$sum": ["$customLocationsCount", "$contentLocationsCount"]
                }
            }
        },
        {"$addFields": {"isPool": {"$toBool": "$type"}}},
        {"$addFields": {"currencyString": {"$toString": "$pricingCurrency"}}},
        {
            "$addFields": {
                "pricingCurrencyUsed": {
                    "$replaceAll": {
                        "input": "$currencyString",
                        "find": "0",
                        "replacement": "EUR",
                    }
                }
            }
        },
        {
            "$addFields": {
                "pricingCurrencyUsed": {
                    "$replaceAll": {
                        "input": "$pricingCurrencyUsed",
                        "find": "1",
                        "replacement": "USD",
                    }
                }
            }
        },
        {"$addFields": {"plainCurrencyString": {"$toString": "$currency"}}},
        {
            "$addFields": {
                "plainCurrencyUsed": {
                    "$replaceAll": {
                        "input": "$plainCurrencyString",
                        "find": "0",
                        "replacement": "EUR",
                    }
                }
            }
        },
        {
            "$addFields": {
                "plainCurrencyUsed": {
                    "$replaceAll": {
                        "input": "$plainCurrencyUsed",
                        "find": "1",
                        "replacement": "USD",
                    }
                }
            }
        },
        {
            "$addFields": {
                "passengersTypes": {
                    "$map": {
                        "input": "$passengers",
                        "as": "p",
                        "in": {"$toString": ["$$p.type"]},
                    }
                }
            }
        },
        {"$unwind": {"path": "$requestHeader", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "passenger0": {"$arrayElemAt": ["$passengersTypes", 0.0]},
                "passenger1": {"$arrayElemAt": ["$passengersTypes", 1.0]},
                "passenger2": {"$arrayElemAt": ["$passengersTypes", 2.0]},
                "passenger3": {"$arrayElemAt": ["$passengersTypes", 3.0]},
                "passenger4": {"$arrayElemAt": ["$passengersTypes", 4.0]},
                "passenger5": {"$arrayElemAt": ["$passengersTypes", 5.0]},
                "passenger6": {"$arrayElemAt": ["$passengersTypes", 6.0]},
                "passenger7": {"$arrayElemAt": ["$passengersTypes", 7.0]},
                "passenger8": {"$arrayElemAt": ["$passengersTypes", 8.0]},
            }
        },
        {
            "$addFields": {
                "additionalTravelTime": {
                    "$map": {
                        "input": "$customLocations",
                        "as": "cl",
                        "in": {"$toInt": ["$$cl.duration"]},
                    }
                }
            }
        },
        {
            "$addFields": {
                "stopTime0": {"$arrayElemAt": ["$additionalTravelTime", 0.0]},
                "stopTime1": {"$arrayElemAt": ["$additionalTravelTime", 1.0]},
                "stopTime2": {"$arrayElemAt": ["$additionalTravelTime", 2.0]},
                "stopTime3": {"$arrayElemAt": ["$additionalTravelTime", 3.0]},
                "stopTime4": {"$arrayElemAt": ["$additionalTravelTime", 4.0]},
                "stopTime5": {"$arrayElemAt": ["$additionalTravelTime", 5.0]},
                "stopTime6": {"$arrayElemAt": ["$additionalTravelTime", 6.0]},
            }
        },
        {
            "$addFields": {
                "travelTime": {
                    "$map": {
                        "input": "$contentLocations",
                        "as": "cl",
                        "in": {"$toInt": ["$$cl.duration"]},
                    }
                }
            }
        },
        {
            "$addFields": {
                "time0": {"$arrayElemAt": ["$travelTime", 0.0]},
                "time1": {"$arrayElemAt": ["$travelTime", 1.0]},
                "time2": {"$arrayElemAt": ["$travelTime", 2.0]},
                "time3": {"$arrayElemAt": ["$travelTime", 3.0]},
                "time4": {"$arrayElemAt": ["$travelTime", 4.0]},
                "time5": {"$arrayElemAt": ["$travelTime", 5.0]},
                "time6": {"$arrayElemAt": ["$travelTime", 6.0]},
            }
        },
        {
            "$addFields": {
                "totalDuration": {
                    "$sum": [
                        "$time0",
                        "$time1",
                        "$time2",
                        "$time3",
                        "$time4",
                        "$time5",
                        "$time6",
                        "$stopTime0",
                        "$stopTime1",
                        "$stopTime2",
                        "$stopTime3",
                        "$stopTime4",
                        "$stopTime5",
                        "$stopTime6",
                    ]
                }
            }
        },
        {
            "$addFields": {
                "isCustomLocation": {"$ne": [{"$size": "$customLocations"}, 0.0]}
            }
        },
        {"$unwind": {"path": "$passengers", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"extraLuggageCount": {"$sum": "$passengers.luggage"}}},
        {
            "$addFields": {
                "totalLuggage": {"$sum": ["$extraLuggageCount", "$passengersCount"]}
            }
        },
        {
            "$addFields": {
                "vehiclesString": {"$toString": {"$arrayElemAt": ["$vehicles", 0.0]}}
            }
        },
        {"$addFields": {"paymentMethodString": {"$toString": "$paymentMethod"}}},
        {
            "$addFields": {
                "vehiclesString": {
                    "$replaceAll": {
                        "input": "$vehiclesString",
                        "find": "0",
                        "replacement": "sedan",
                    }
                }
            }
        },
        {
            "$addFields": {
                "paymentMethodString": {
                    "$replaceAll": {
                        "input": "$paymentMethodString",
                        "find": "0",
                        "replacement": "cash",
                    }
                }
            }
        },
        {
            "$addFields": {
                "paymentMethodString": {
                    "$replaceAll": {
                        "input": "$paymentMethodString",
                        "find": "1",
                        "replacement": "prepaid online",
                    }
                }
            }
        },
        {
            "$addFields": {
                "paymentMethodString": {
                    "$replaceAll": {
                        "input": "$paymentMethodString",
                        "find": "2",
                        "replacement": "API",
                    }
                }
            }
        },
        {
            "$addFields": {
                "vehiclesString": {
                    "$replaceAll": {
                        "input": "$vehiclesString",
                        "find": "1",
                        "replacement": "mpv",
                    }
                }
            }
        },
        {
            "$addFields": {
                "vehiclesString": {
                    "$replaceAll": {
                        "input": "$vehiclesString",
                        "find": "2",
                        "replacement": "van",
                    }
                }
            }
        },
        {
            "$addFields": {
                "vehiclesString": {
                    "$replaceAll": {
                        "input": "$vehiclesString",
                        "find": "3",
                        "replacement": "luxury sedan",
                    }
                }
            }
        },
        {
            "$addFields": {
                "vehiclesString": {
                    "$replaceAll": {
                        "input": "$vehiclesString",
                        "find": "4",
                        "replacement": "shuttle",
                    }
                }
            }
        },
        {
            "$addFields": {
                "vehiclesString": {
                    "$replaceAll": {
                        "input": "$vehiclesString",
                        "find": "100",
                        "replacement": "sedan lite",
                    }
                }
            }
        },
        {"$match": {"passengers.type": "lead"}},
        {
            "$lookup": {
                "from": "locations",
                "localField": "originLocationId",
                "foreignField": "_id",
                "as": "originLocation",
            }
        },
        {
            "$lookup": {
                "from": "routes",
                "localField": "routeId",
                "foreignField": "_id",
                "as": "routes",
            }
        },
        {
            "$lookup": {
                "from": "chargebacks",
                "localField": "_id",
                "foreignField": "orderId",
                "as": "chargebacks",
            }
        },
        {
            "$lookup": {
                "from": "payments",
                "let": {"paymentId": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$and": [
                                {"$expr": {"$eq": ["$orderId", "$$paymentId"]}},
                                {"fulfilledAt": {"$ne": None}},
                                {"failedAt": {"$eq": None}},
                                {"chargedBackAt": {"$eq": None}},
                            ]
                        }
                    }
                ],
                "as": "payments",
            }
        },
        {
            "$addFields": {
                "payments": {
                    "$cond": {
                        "if": {"$gt": [{"$size": "$payments"}, 0.0]},
                        "then": {"$arrayElemAt": ["$payments", 0.0]},
                        "else": None,
                    }
                }
            }
        },
        {"$unwind": {"path": "$payments", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"isLite": {"$in": ["100", "$vehicles"]}}},
        {
            "$addFields": {
                "vehiclesFees": {
                    "$map": {
                        "input": "$routes.vehicleTypesPricesFees",
                        "as": "rt",
                        "in": {
                            "$map": {
                                "input": "$$rt.fee",
                                "as": "f",
                                "in": {"$toInt": "$$f"},
                            }
                        },
                    }
                }
            }
        },
        {
            "$addFields": {
                "vehicles": {
                    "$map": {
                        "input": "$routes.vehicleTypesPricesFees",
                        "as": "rt",
                        "in": {
                            "$map": {
                                "input": "$$rt.price",
                                "as": "f",
                                "in": {"$toInt": "$$f"},
                            }
                        },
                    }
                }
            }
        },
        {"$unwind": {"path": "$vehicles", "preserveNullAndEmptyArrays": True}},
        {"$unwind": {"path": "$vehiclesFees", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "sedanPrice": {"$arrayElemAt": ["$vehicles", 0.0]},
                "mpvPrice": {"$arrayElemAt": ["$vehicles", 1.0]},
                "vanPrice": {"$arrayElemAt": ["$vehicles", 2.0]},
                "luxurySedan": {"$arrayElemAt": ["$vehicles", 3.0]},
            }
        },
        {
            "$addFields": {
                "sedanFee": {"$arrayElemAt": ["$vehiclesFees", 0.0]},
                "mpvFee": {"$arrayElemAt": ["$vehiclesFees", 1.0]},
                "vanFee": {"$arrayElemAt": ["$vehiclesFees", 2.0]},
                "luxurySedanFee": {"$arrayElemAt": ["$vehiclesFees", 3.0]},
            }
        },
        {
            "$addFields": {
                "tollFeeTwice": {
                    "$multiply": [{"$arrayElemAt": ["$routes.tollFee", 0.0]}, 2.0]
                },
                "tollPriceTwice": {
                    "$multiply": [{"$arrayElemAt": ["$routes.tollPrice", 0.0]}, 2.0]
                },
            }
        },
        {
            "$addFields": {
                "sedanTotalPrice": {
                    "$sum": [
                        "$sedanPrice",
                        "$sedanFee",
                        "$routes.additionalFee",
                        "$routes.additionalPrice",
                        "$tollFeeTwice",
                        "$tollPriceTwice",
                    ]
                }
            }
        },
        {
            "$addFields": {
                "mpvTotalPrice": {
                    "$sum": [
                        "$mpvPrice",
                        "$mpvFee",
                        "$routes.additionalFee",
                        "$routes.additionalPrice",
                        "$tollFeeTwice",
                        "$tollPriceTwice",
                    ]
                }
            }
        },
        {
            "$addFields": {
                "vanTotalPrice": {
                    "$sum": [
                        "$vanPrice",
                        "$vanFee",
                        "$routes.additionalFee",
                        "$routes.additionalPrice",
                        "$tollFeeTwice",
                        "$tollPriceTwice",
                    ]
                }
            }
        },
        {
            "$addFields": {
                "luxurySedanTotalPrice": {
                    "$sum": [
                        "$luxurySedan",
                        "$luxurySedanFee",
                        "$routes.additionalFee",
                        "$routes.additionalPrice",
                        "$tollFeeTwice",
                        "$tollPriceTwice",
                    ]
                }
            }
        },
        {"$unwind": {"path": "$originLocation", "preserveNullAndEmptyArrays": True}},
        {
            "$unwind": {
                "path": "$originLocation.name",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "countries",
                "localField": "originLocation.countryId",
                "foreignField": "_id",
                "as": "originLocationCountry",
            }
        },
        {
            "$unwind": {
                "path": "$originLocationCountry",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$originLocationCountry.englishName",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "locations",
                "localField": "destinationLocationId",
                "foreignField": "_id",
                "as": "destinationLocation",
            }
        },
        {
            "$unwind": {
                "path": "$destinationLocation",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$destinationLocation.name",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "countries",
                "localField": "destinationLocation.countryId",
                "foreignField": "_id",
                "as": "destinationLocationCountry",
            }
        },
        {
            "$unwind": {
                "path": "$destinationLocationCountry",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$destinationLocationCountry.englishName",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "countries",
                "localField": "passengers.countryId",
                "foreignField": "_id",
                "as": "leadCountry",
            }
        },
        {"$unwind": {"path": "$leadCountry", "preserveNullAndEmptyArrays": True}},
        {
            "$unwind": {
                "path": "$leadCountry.englishName",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "userId",
                "foreignField": "_id",
                "as": "userCollection",
            }
        },
        {"$unwind": {"path": "$userCollection", "preserveNullAndEmptyArrays": True}},
        {
            "$unwind": {
                "path": "$userCollection.affiliatePartner",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$userCollection.travelAgent",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$userCollection.travelAgent.agentId",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$userCollection.travelAgent.ownerId",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$userCollection.travelAgent.approvedAt",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$userCollection.travelAgent.discountCoefficient",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$userCollection.suspiciousCCActivity",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "createdBy",
                "foreignField": "_id",
                "as": "createdByUser",
            }
        },
        {"$unwind": {"path": "$createdByUser", "preserveNullAndEmptyArrays": True}},
        {
            "$unwind": {
                "path": "$createdByUser.firstName",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$createdByUser.lastName",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$createdByUser.travelAgent",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$createdByUser.travelAgent.agentId",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "userId",
                "foreignField": "_id",
                "as": "customerCollection",
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "affiliatePartnerId",
                "foreignField": "_id",
                "as": "partnerCollection",
            }
        },
        {
            "$unwind": {
                "path": "$customerCollection",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {"$unwind": {"path": "$partnerCollection", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"isGroup": {"$ifNull": ["$customerCollection.groupIds", []]}}},
        {"$addFields": {"isGroup": {"$size": "$isGroup"}}},
        {
            "$addFields": {
                "passenger0Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger0", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger1Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger1", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger2Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger2", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger3Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger3", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger4Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger4", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger5Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger5", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger6Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger6", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger7Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger7", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
                "passenger8Child": {
                    "$cond": {
                        "if": {"$eq": ["$passenger8", "child"]},
                        "then": True,
                        "else": False,
                    }
                },
            }
        },
        {
            "$lookup": {
                "from": "countries",
                "localField": "pricingCountryId",
                "foreignField": "_id",
                "as": "pricingCountryCollection",
            }
        },
        {
            "$unwind": {
                "path": "$pricingCountryCollection",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$pricingCountryCollection.englishName",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$addFields": {
                "createdAt": {"$toDate": "$createdAt"},
                "sumOfChargebacks": {
                    "$reduce": {
                        "input": "$chargebacks",
                        "initialValue": 0.0,
                        "in": {"$add": ["$$value", "$$this.amount"]},
                    }
                },
            }
        },
        {
            "$addFields": {
                "travelAgentId": {
                    "$cond": [
                        {
                            "$and": [
                                {
                                    "$not": [
                                        {
                                            "$regexMatch": {
                                                "input": "$userCollection.travelAgent.agentId",
                                                "regex": "FROSCH",
                                            }
                                        }
                                    ]
                                },
                                {
                                    "$not": [
                                        {
                                            "$regexMatch": {
                                                "input": "$userCollection.travelAgent.agentId",
                                                "regex": "INTRAIL",
                                            }
                                        }
                                    ]
                                },
                                {
                                    "$not": [
                                        {
                                            "$regexMatch": {
                                                "input": "$userCollection.travelAgent.agentId",
                                                "regex": "Tripmasters",
                                            }
                                        }
                                    ]
                                },
                            ]
                        },
                        "$userCollection.travelAgent.agentId",
                        None,
                    ]
                },
                "travelAgentIdHostAgencies": {
                    "$cond": [
                        {
                            "$or": [
                                {
                                    "$regexMatch": {
                                        "input": "$userCollection.travelAgent.agentId",
                                        "regex": "FROSCH",
                                    }
                                },
                                {
                                    "$regexMatch": {
                                        "input": "$userCollection.travelAgent.agentId",
                                        "regex": "INTRAIL",
                                    }
                                },
                                {
                                    "$regexMatch": {
                                        "input": "$userCollection.travelAgent.agentId",
                                        "regex": "Tripmasters",
                                    }
                                },
                            ]
                        },
                        "$userCollection.travelAgent.agentId",
                        None,
                    ]
                },
                "travelAgentOwnerId": "$userCollection.travelAgent.ownerId",
            }
        },
        {
            "$addFields": {
                "travelAgentOwnerId": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$eq": [
                                        "$travelAgentOwnerId",
                                        "2c7d5c78-97cd-4796-a493-ca4bc68fde47",
                                    ]
                                },
                                "then": "Sašenka Mamrillová",
                            },
                            {
                                "case": {
                                    "$eq": [
                                        "$travelAgentOwnerId",
                                        "8fe652a6-209e-4f83-b6b4-ec43c0a74c9c",
                                    ]
                                },
                                "then": "Sašenka Mamrillová",
                            },
                            {
                                "case": {
                                    "$eq": [
                                        "$travelAgentOwnerId",
                                        "4fb118b3-f562-4a10-8b0c-3d10d9c09950",
                                    ]
                                },
                                "then": "Jan Toloch",
                            },
                        ],
                        "default": "$travelAgentOwnerId",
                    }
                }
            }
        },
        {
            "$lookup": {
                "from": "travelData",
                "let": {
                    "originLocationId": "$originLocationId",
                    "destinationLocationId": "$destinationLocationId",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$originId", "$$originLocationId"]},
                                    {
                                        "$eq": [
                                            "$destinationId",
                                            "$$destinationLocationId",
                                        ]
                                    },
                                ]
                            }
                        }
                    }
                ],
                "as": "matchedData",
            }
        },
        {"$addFields": {"matchedDocument": {"$arrayElemAt": ["$matchedData", 0]}}},
        {
            "$addFields": {
                "travelDataDuration": "$matchedDocument.duration",
                "travelDataDistance": "$matchedDocument.distance",
            }
        },
        {
            "$addFields": {
                "hasAdditionalStop": {
                    "$cond": {
                        "if": {"$gte": ["$additionalStopCount", 1]},
                        "then": True,
                        "else": False,
                    }
                }
            }
        },
        {
            "$project": {
                "vehicles": "$vehicleTypesPricesFees.vehicleType",
                "travelAgentOwnerId": 1.0,
                "isLite": 1.0,
                "hasAdditionalStop": 1.0,
                "travelDataDuration": 1.0,
                "travelDataDistance": 1.0,
                "fee": 1.0,
                "createdAt": 1.0,
                "departureAt": 1.0,
                "confirmedAt": 1.0,
                "cancelledAt": 1.0,
                "acceptedAt": 1.0,
                "declinedAt": 1.0,
                "draftedAt": 1.0,
                "pricingCurrencyUsed": 1.0,
                "plainCurrencyUsed": 1.0,
                "currencyRate": 1.0,
                "additionalFee": 1.0,
                "additionalPrice": 1.0,
                "paymentMethodString": 1.0,
                "originCountry": "$originLocationCountry.englishName",
                "originLocation": "$originLocation.name",
                "destinationCountry": "$destinationLocationCountry.englishName",
                "destinationLocation": "$destinationLocation.name",
                "isCustomLocation": 1.0,
                "totalStopsCount": 1.0,
                "isPool": 1.0,
                "createdBy": 1.0,
                "leadUserId": "$userId",
                "leadFullName": {
                    "$concat": ["$passengers.firstName", " ", "$passengers.lastName"]
                },
                "leadBirthday": "$passengers.birthdayAt",
                "leadEmail": "$passengers.email",
                "leadCountry": "$leadCountry.englishName",
                "passengersCount": 1.0,
                "potentialFraud": 1.0,
                "potentialCCFraud": "$userCollection.suspiciousCCActivity",
                "vehicleType": "$vehiclesString",
                "vehiclesCount": 1.0,
                "travelAgentId": 1.0,
                "travelAgentIdHostAgencies": 1.0,
                "travelAgentApprovedAt": "$userCollection.travelAgent.approvedAt",
                "travelAgentDiscount": {
                    "$ifNull": ["$userCollection.travelAgent.discountCoefficient", 0.0]
                },
                "partnerId": 1.0,
                "totalPrice": 1.0,
                "routeName": {
                    "$concat": [
                        "$originLocation.name",
                        " ",
                        "to",
                        " ",
                        "$destinationLocation.name",
                    ]
                },
                "totalDuration": 1.0,
                "departureDateChangedLessThan24hUntilDeparture": 1.0,
                "createdDepartureDiff": {
                    "$divide": [
                        {"$subtract": ["$departureAt", "$createdAt"]},
                        86400000.0,
                    ]
                },
                "orderUserAgent": "$requestHeader.userAgent",
                "luggageCount": "$totalLuggage",
                "passenger0": 1.0,
                "passenger1": 1.0,
                "passenger2": 1.0,
                "passenger3": 1.0,
                "passenger4": 1.0,
                "passenger5": 1.0,
                "passenger6": 1.0,
                "passenger7": 1.0,
                "passenger8": 1.0,
                "passenger0Child": 1.0,
                "passenger1Child": 1.0,
                "passenger2Child": 1.0,
                "passenger3Child": 1.0,
                "passenger4Child": 1.0,
                "passenger5Child": 1.0,
                "passenger6Child": 1.0,
                "passenger7Child": 1.0,
                "passenger8Child": 1.0,
                "createdByFullName": {
                    "$concat": [
                        "$createdByUser.firstName",
                        " ",
                        "$createdByUser.lastName",
                    ]
                },
                "createdByTravelAgent": "$createdByUser.travelAgent.agentId",
                "affiliatePartnerId": 1.0,
                "customerFullName": {
                    "$concat": [
                        "$customerCollection.firstName",
                        " ",
                        "$customerCollection.lastName",
                    ]
                },
                "isGroup": 1.0,
                "pricingCountryName": "$pricingCountryCollection.englishName",
                "routeId": 1.0,
                "userId": 1.0,
                "userCreatedAt": "$userCollection.createdAt",
                "customerNote": "$partnerCollection.customerNote",
                "sedanTotalPrice": 1.0,
                "mpvTotalPrice": 1.0,
                "vanTotalPrice": 1.0,
                "luxurySedanTotalPrice": 1.0,
                "userAffiliatePartnerId": "$userCollection.affiliatePartner.partnerId",
                "sumOfChargebacks": 1.0,
                "sumOfPenalties": 1.0,
                "sumOfCompensations": 1.0,
                "price": 1.0,
                "isBusinessTrip": 1.0,
                "paymentType": "$payments.type",
                "driverRating": 1.0,
                "ipAddress": "$requestHeader.remoteAddress",
                "userAgent": "$requestHeader.userAgent",
                "sumOfDiscountsFee": 1.0,
                "sumOfSubsidies": 1.0,
            }
        },
        {
            "$project": {
                "vehicles": 1.0,
                "isLite": 1.0,
                "travelAgentOwnerId": 1.0,
                "hasAdditionalStop": 1.0,
                "travelDataDuration": 1.0,
                "travelDataDistance": 1.0,
                "fee": 1.0,
                "createdAt": 1.0,
                "departureAt": 1.0,
                "confirmedAt": 1.0,
                "cancelledAt": 1.0,
                "acceptedAt": 1.0,
                "declinedAt": 1.0,
                "draftedAt": 1.0,
                "pricingCurrencyUsed": 1.0,
                "plainCurrencyUsed": 1.0,
                "currencyRate": 1.0,
                "additionalFee": 1.0,
                "additionalPrice": 1.0,
                "paymentMethodString": 1.0,
                "originCountry": 1.0,
                "originLocation": 1.0,
                "destinationCountry": 1.0,
                "destinationLocation": 1.0,
                "totalStopsCount": 1.0,
                "isCustomLocation": 1.0,
                "isPool": 1.0,
                "createdBy": 1.0,
                "leadUserId": 1.0,
                "leadFullName": 1.0,
                "leadBirthday": 1.0,
                "leadEmail": 1.0,
                "leadCountry": 1.0,
                "passengersCount": 1.0,
                "potentialFraud": 1.0,
                "potentialCCFraud": 1.0,
                "vehicleType": 1.0,
                "vehiclesCount": 1.0,
                "travelAgentId": 1.0,
                "travelAgentDiscount": 1.0,
                "travelAgentApprovedAt": 1.0,
                "partnerId": 1.0,
                "totalPrice": 1.0,
                "routeName": 1.0,
                "totalDuration": 1.0,
                "isLastMinute24": "$departureDateChangedLessThan24hUntilDeparture",
                "createdDepartureDiff": 1.0,
                "orderUserAgent": 1.0,
                "luggageCount": 1.0,
                "passenger0": 1.0,
                "passenger1": 1.0,
                "passenger2": 1.0,
                "passenger3": 1.0,
                "passenger4": 1.0,
                "passenger5": 1.0,
                "passenger6": 1.0,
                "passenger7": 1.0,
                "passenger8": 1.0,
                "passenger0Child": 1.0,
                "passenger1Child": 1.0,
                "passenger2Child": 1.0,
                "passenger3Child": 1.0,
                "passenger4Child": 1.0,
                "passenger5Child": 1.0,
                "passenger6Child": 1.0,
                "passenger7Child": 1.0,
                "passenger8Child": 1.0,
                "createdByFullName": 1.0,
                "createdByTravelAgent": 1.0,
                "affiliatePartnerId": 1.0,
                "customerFullName": 1.0,
                "isGroup": 1.0,
                "pricingCountryName": 1.0,
                "routeId": 1.0,
                "userId": 1.0,
                "userCreatedAt": 1.0,
                "customerNote": 1.0,
                "sedanTotalPrice": 1.0,
                "mpvTotalPrice": 1.0,
                "vanTotalPrice": 1.0,
                "luxurySedanTotalPrice": 1.0,
                "userAffiliatePartnerId": 1.0,
                "sumOfChargebacks": 1.0,
                "sumOfPenalties": 1.0,
                "sumOfCompensations": 1.0,
                "price": 1.0,
                "isBusinessTrip": 1.0,
                "paymentType": 1.0,
                "driverRating": 1.0,
                "ipAddress": 1.0,
                "userAgent": 1.0,
                "sumOfDiscountsFee": 1.0,
                "sumOfSubsidies": 1.0,
                "travelAgentIdHostAgencies": 1.0,
                "status": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$confirmedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$draftedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$draftedAt", None]},
                                            ]
                                        },
                                    ]
                                },
                                "then": 0.0,
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$confirmedAt", None]},
                                            ]
                                        },
                                    ]
                                },
                                "then": 1.0,
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$declinedAt", None]},
                                            ]
                                        },
                                    ]
                                },
                                "then": 2.0,
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$confirmedAt", None]},
                                            ]
                                        },
                                    ]
                                },
                                "then": 3.0,
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$cancelledAt", None]},
                                            ]
                                        }
                                    ]
                                },
                                "then": 4.0,
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$confirmedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$draftedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$draftedAt", None]},
                                            ]
                                        },
                                    ]
                                },
                                "then": 5.0,
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$confirmedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$draftedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$draftedAt", None]},
                                            ]
                                        },
                                    ]
                                },
                                "then": 6.0,
                            },
                        ],
                        "default": "not recognized",
                    }
                },
            }
        },
        {"$addFields": {"orderStatusString": {"$toString": "$status"}}},
        {
            "$addFields": {
                "orderStatusString": {
                    "$replaceAll": {
                        "input": "$orderStatusString",
                        "find": "0",
                        "replacement": "pending",
                    }
                }
            }
        },
        {
            "$addFields": {
                "orderStatusString": {
                    "$replaceAll": {
                        "input": "$orderStatusString",
                        "find": "1",
                        "replacement": "accepted",
                    }
                }
            }
        },
        {
            "$addFields": {
                "orderStatusString": {
                    "$replaceAll": {
                        "input": "$orderStatusString",
                        "find": "2",
                        "replacement": "declined",
                    }
                }
            }
        },
        {
            "$addFields": {
                "orderStatusString": {
                    "$replaceAll": {
                        "input": "$orderStatusString",
                        "find": "3",
                        "replacement": "confirmed",
                    }
                }
            }
        },
        {
            "$addFields": {
                "orderStatusString": {
                    "$replaceAll": {
                        "input": "$orderStatusString",
                        "find": "4",
                        "replacement": "cancelled",
                    }
                }
            }
        },
        {
            "$addFields": {
                "orderStatusString": {
                    "$replaceAll": {
                        "input": "$orderStatusString",
                        "find": "5",
                        "replacement": "undefined",
                    }
                }
            }
        },
        {
            "$addFields": {
                "orderStatusString": {
                    "$replaceAll": {
                        "input": "$orderStatusString",
                        "find": "6",
                        "replacement": "draft",
                    }
                }
            }
        },
        {"$addFields": {"b2bMargin0": {"$subtract": ["$totalPrice", "$price"]}}},
        {
            "$addFields": {
                "b2bMargin2": {"$subtract": ["$b2bMargin0", "$sumOfChargebacks"]}
            }
        },
        {
            "$addFields": {
                "b2bMargin3": {"$subtract": ["$b2bMargin2", "$sumOfSubsidies"]}
            }
        },
        {
            "$addFields": {
                "b2bMargin4": {"$subtract": ["$b2bMargin3", "$sumOfCompensations"]}
            }
        },
        {"$addFields": {"b2bMargin": {"$add": ["$b2bMargin4", "$sumOfPenalties"]}}},
        {"$addFields": {"b2bMarginAlt0": {"$subtract": ["$totalPrice", "$price"]}}},
        {
            "$addFields": {
                "b2bMarginAlt": {"$subtract": ["$b2bMarginAlt0", "$sumOfChargebacks"]}
            }
        },
        {
            "$addFields": {
                "userOS": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Windows NT 10\\.0",
                                    }
                                },
                                "then": "Windows 10",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Windows NT 6\\.2",
                                    }
                                },
                                "then": "Windows 8",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Windows NT 5\\.1",
                                    }
                                },
                                "then": "Windows XP",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Windows NT 6\\.3",
                                    }
                                },
                                "then": "Windows 8.1",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Windows NT 10\\.0;.*Win64;.*x64",
                                    }
                                },
                                "then": "Windows 11",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "(?:X11;|Linux)",
                                    }
                                },
                                "then": {
                                    "$cond": {
                                        "if": {
                                            "$regexMatch": {
                                                "input": "$userAgent",
                                                "regex": "arm",
                                            }
                                        },
                                        "then": "Android",
                                        "else": "Android",
                                    }
                                },
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Macintosh",
                                    }
                                },
                                "then": "macOS",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Android",
                                    }
                                },
                                "then": "Android",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "(iPhone|iPod|iPad).*CPU.*",
                                    }
                                },
                                "then": "iOS",
                            },
                        ],
                        "default": "Unknown OS",
                    }
                },
                "userBrowser": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Chrome",
                                    }
                                },
                                "then": "Chrome",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Firefox",
                                    }
                                },
                                "then": "Firefox",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Safari",
                                    }
                                },
                                "then": "Safari",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Edge",
                                    }
                                },
                                "then": "Edge",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "MSIE",
                                    }
                                },
                                "then": "Internet Explorer",
                            },
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$userAgent",
                                        "regex": "Brave",
                                    }
                                },
                                "then": "Brave",
                            },
                        ],
                        "default": "Unknown Browser",
                    }
                },
            }
        },
        {
            "$addFields": {
                "paymentMethodB2B": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$or": [
                                        {
                                            "$ne": [
                                                {"$ifNull": ["$travelAgentId", ""]},
                                                "",
                                            ]
                                        },
                                        {"$ne": [{"$ifNull": ["$partnerId", ""]}, ""]},
                                        {
                                            "$ne": [
                                                {
                                                    "$ifNull": [
                                                        "$affiliatePartnerId",
                                                        "",
                                                    ]
                                                },
                                                "",
                                            ]
                                        },
                                        {
                                            "$ne": [
                                                {
                                                    "$ifNull": [
                                                        "$travelAgentIdHostAgencies",
                                                        "",
                                                    ]
                                                },
                                                "",
                                            ]
                                        },
                                    ]
                                },
                                "then": "B2B",
                            },
                            {
                                "case": {
                                    "$eq": ["$paymentMethodString", "prepaid online"]
                                },
                                "then": "privatePrepaidOnline",
                            },
                            {
                                "case": {"$eq": ["$paymentMethodString", "cash"]},
                                "then": "privateCash",
                            },
                        ],
                        "default": "other",
                    }
                }
            }
        },
        {
            "$project": {
                "vehicles": 1.0,
                "isLite": 1.0,
                "travelAgentOwnerId": 1.0,
                "hasAdditionalStop": 1.0,
                "travelDataDuration": 1.0,
                "travelDataDistance": 1.0,
                "fee": 1.0,
                "createdAt": 1.0,
                "departureAt": 1.0,
                "confirmedAt": 1.0,
                "cancelledAt": 1.0,
                "acceptedAt": 1.0,
                "declinedAt": 1.0,
                "draftedAt": 1.0,
                "pricingCurrencyUsed": 1.0,
                "plainCurrencyUsed": 1.0,
                "currencyRate": 1.0,
                "additionalFee": 1.0,
                "additionalPrice": 1.0,
                "paymentMethodString": 1.0,
                "originCountry": 1.0,
                "originLocation": 1.0,
                "destinationCountry": 1.0,
                "destinationLocation": 1.0,
                "totalStopsCount": 1.0,
                "isCustomLocation": 1.0,
                "isPool": 1.0,
                "createdBy": 1.0,
                "leadUserId": 1.0,
                "leadFullName": 1.0,
                "leadBirthday": 1.0,
                "leadEmail": 1.0,
                "leadCountry": 1.0,
                "passengersCount": 1.0,
                "potentialFraud": 1.0,
                "potentialCCFraud": 1.0,
                "vehicleType": 1.0,
                "vehiclesCount": 1.0,
                "travelAgentId": 1.0,
                "travelAgentDiscount": 1.0,
                "travelAgentApprovedAt": 1.0,
                "partnerId": 1.0,
                "totalPrice": 1.0,
                "routeName": 1.0,
                "totalDuration": 1.0,
                "isLastMinute24": 1.0,
                "createdDepartureDiff": 1.0,
                "orderUserAgent": 1.0,
                "luggageCount": 1.0,
                "passenger0": 1.0,
                "passenger1": 1.0,
                "passenger2": 1.0,
                "passenger3": 1.0,
                "passenger4": 1.0,
                "passenger5": 1.0,
                "passenger6": 1.0,
                "passenger7": 1.0,
                "passenger8": 1.0,
                "passenger0Child": 1.0,
                "passenger1Child": 1.0,
                "passenger2Child": 1.0,
                "passenger3Child": 1.0,
                "passenger4Child": 1.0,
                "passenger5Child": 1.0,
                "passenger6Child": 1.0,
                "passenger7Child": 1.0,
                "passenger8Child": 1.0,
                "createdByFullName": 1.0,
                "createdByTravelAgent": 1.0,
                "affiliatePartnerId": 1.0,
                "customerFullName": 1.0,
                "isGroup": 1.0,
                "pricingCountryName": 1.0,
                "routeId": 1.0,
                "userId": 1.0,
                "status": 1.0,
                "orderStatus": "$orderStatusString",
                "userCreatedAt": 1.0,
                "customerNote": 1.0,
                "sedanTotalPrice": 1.0,
                "mpvTotalPrice": 1.0,
                "vanTotalPrice": 1.0,
                "luxurySedanTotalPrice": 1.0,
                "userAffiliatePartnerId": 1.0,
                "sumOfChargebacks": 1.0,
                "sumOfPenalties": 1.0,
                "sumOfCompensations": 1.0,
                "price": 1.0,
                "b2bMargin": 1.0,
                "b2bMarginAlt": 1.0,
                "isBusinessTrip": 1.0,
                "paymentType": 1.0,
                "driverRating": 1.0,
                "ipAddress": 1.0,
                "userAgent": 1.0,
                "sumOfDiscountsFee": 1.0,
                "sumOfSubsidies": 1.0,
                "userBrowser": 1.0,
                "userOS": 1.0,
                "travelAgentIdHostAgencies": 1.0,
                "paymentMethodB2B": 1.0,
            }
        },
    ]
