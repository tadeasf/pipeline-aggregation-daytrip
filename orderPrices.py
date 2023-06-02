def pipeline_no_table():
    return [
        {"$sort": {"createdAt": 1}},
        {"$limit": 6500},
        {
            "$lookup": {
                "from": "discounts",
                "localField": "discountIds",
                "foreignField": "_id",
                "as": "discounts",
            }
        },
        {
            "$lookup": {
                "from": "locations",
                "localField": "originLocationId",
                "foreignField": "_id",
                "as": "originLocationCollection",
            }
        },
        {
            "$lookup": {
                "from": "locations",
                "localField": "destinationLocationId",
                "foreignField": "_id",
                "as": "destinationLocationCollection",
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
                "from": "countries",
                "localField": "originLocationCollection.countryId",
                "foreignField": "_id",
                "as": "originCountryCollection",
            }
        },
        {
            "$lookup": {
                "from": "countries",
                "localField": "destinationLocationCollection.countryId",
                "foreignField": "_id",
                "as": "destinationCountryCollection",
            }
        },
        {
            "$lookup": {
                "from": "customerFeedbacks",
                "localField": "_id",
                "foreignField": "orderId",
                "as": "feedbacks",
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
                "from": "users",
                "localField": "assignation.userId",
                "foreignField": "_id",
                "as": "drivers",
            }
        },
        {
            "$lookup": {
                "from": "vehicles",
                "localField": "drivers._id",
                "foreignField": "ownerUserId",
                "as": "driversVehicles",
            }
        },
        {
            "$lookup": {
                "from": "vehicleModels",
                "localField": "driversVehicles.modelId",
                "foreignField": "_id",
                "as": "driversVehiclesModels",
            }
        },
        {
            "$lookup": {
                "from": "vehicleMakes",
                "localField": "driversVehiclesModels.makeId",
                "foreignField": "_id",
                "as": "driversVehiclesModelsMakes",
            }
        },
        {
            "$lookup": {
                "from": "countries",
                "localField": "pricingCountryId",
                "foreignField": "_id",
                "as": "pricingCountry",
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
            "$lookup": {
                "from": "payments",
                "localField": "_id",
                "foreignField": "orderId",
                "as": "paymentsFull",
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
        {
            "$lookup": {
                "from": "users",
                "localField": "affiliatePartnerId",
                "foreignField": "_id",
                "as": "partnerCollection",
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
        {"$addFields": {"order": "$$ROOT"}},
        {
            "$addFields": {
                "passengersCount": {"$size": "$passengers"},
                "vehiclesCount": {"$size": "$vehicles"},
                "additionalStopCount": {"$size": "$customLocations"},
                "sumOfChargebacks": {
                    "$reduce": {
                        "input": "$chargebacks",
                        "initialValue": 0.0,
                        "in": {"$add": ["$$value", "$$this.amount"]},
                    }
                },
                "payments": {
                    "$cond": {
                        "if": {"$gt": [{"$size": "$payments"}, 0.0]},
                        "then": {"$arrayElemAt": ["$payments", 0.0]},
                        "else": None,
                    }
                },
                "driverRating": {"$avg": "$driverFeedback.textScore"},
                "rating1": {
                    "$size": {
                        "$filter": {
                            "input": {"$ifNull": ["$feedbacks", []]},
                            "as": "feedback",
                            "cond": {"$eq": ["$$feedback.textScore", int(1)]},
                        }
                    }
                },
                "rating2": {
                    "$size": {
                        "$filter": {
                            "input": {"$ifNull": ["$feedbacks", []]},
                            "as": "feedback",
                            "cond": {"$eq": ["$$feedback.textScore", int(2)]},
                        }
                    }
                },
                "rating3": {
                    "$size": {
                        "$filter": {
                            "input": {"$ifNull": ["$feedbacks", []]},
                            "as": "feedback",
                            "cond": {"$eq": ["$$feedback.textScore", int(3)]},
                        }
                    }
                },
                "rating4": {
                    "$size": {
                        "$filter": {
                            "input": {"$ifNull": ["$feedbacks", []]},
                            "as": "feedback",
                            "cond": {"$eq": ["$$feedback.textScore", int(4)]},
                        }
                    }
                },
                "rating5": {
                    "$size": {
                        "$filter": {
                            "input": {"$ifNull": ["$feedbacks", []]},
                            "as": "feedback",
                            "cond": {"$eq": ["$$feedback.textScore", int(5)]},
                        }
                    }
                },
            }
        },
        {
            "$addFields": {
                "paymentType": "$payments.type",
                "vehiclesString": {"$toString": {"$arrayElemAt": ["$vehicles", 0.0]}},
                "driverFirstName0": {"$arrayElemAt": ["$drivers.firstName", 0.0]},
                "driverFirstName1": {"$arrayElemAt": ["$drivers.firstName", 1.0]},
                "driverFirstName2": {"$arrayElemAt": ["$drivers.firstName", 2.0]},
                "driverLastName0": {"$arrayElemAt": ["$drivers.lastName", 0.0]},
                "driverLastName1": {"$arrayElemAt": ["$drivers.lastName", 1.0]},
                "driverLastName2": {"$arrayElemAt": ["$drivers.lastName", 2.0]},
                "driver0Company": {
                    "$arrayElemAt": ["$drivers.driversCompany.name", 0.0]
                },
                "driver1Company": {
                    "$arrayElemAt": ["$drivers.driversCompany.name", 1.0]
                },
                "drive2Company": {
                    "$arrayElemAt": ["$drivers.driversCompany.name", 2.0]
                },
                "vehicleYearOfManufacture0": {
                    "$arrayElemAt": ["$driversVehicles.manufactureYear", 0.0]
                },
                "vehicleYearOfManufacture1": {
                    "$arrayElemAt": ["$driversVehicles.manufactureYear", 1.0]
                },
                "vehicleYearOfManufacture2": {
                    "$arrayElemAt": ["$driversVehicles.manufactureYear", 2.0]
                },
                "vehicleModel0": {"$arrayElemAt": ["$driversVehiclesModels.name", 0.0]},
                "vehicleModel1": {"$arrayElemAt": ["$driversVehiclesModels.name", 1.0]},
                "vehicleModel2": {"$arrayElemAt": ["$driversVehiclesModels.name", 2.0]},
                "vehicleMake0": {
                    "$arrayElemAt": ["$driversVehiclesModelsMakes.name", 0.0]
                },
                "vehicleMake1": {
                    "$arrayElemAt": ["$driversVehiclesModelsMakes.name", 1.0]
                },
                "vehicleMake2": {
                    "$arrayElemAt": ["$driversVehiclesModelsMakes.name", 2.0]
                },
                "sumOfSubsidies": {
                    "$reduce": {
                        "input": "$subsidies",
                        "initialValue": 0.0,
                        "in": {"$sum": ["$$value", "$$this.value"]},
                    }
                },
            }
        },
        {
            "$addFields": {
                "totalPrice": {
                    "$let": {
                        "vars": {
                            "let_key_1": {
                                "$sum": [
                                    {"$ceil": {"$multiply": [2.0, "$order.tollPrice"]}},
                                    {"$ceil": {"$multiply": [2.0, "$order.tollFee"]}},
                                    "$order.additionalPrice",
                                    "$order.additionalFee",
                                ]
                            }
                        },
                        "in": {
                            "$reduce": {
                                "input": "$order.vehicles",
                                "initialValue": 0.0,
                                "in": {
                                    "$let": {
                                        "vars": {"let_key_2": "$$this"},
                                        "in": {
                                            "$sum": [
                                                "$$value",
                                                "$$let_key_1",
                                                {
                                                    "$let": {
                                                        "vars": {
                                                            "let_key_4": {
                                                                "$arrayElemAt": [
                                                                    {
                                                                        "$filter": {
                                                                            "input": "$order.vehicleTypesPricesFees",
                                                                            "as": "key_3",
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$key_3.vehicleType",
                                                                                    "$$let_key_2",
                                                                                ]
                                                                            },
                                                                        }
                                                                    },
                                                                    0.0,
                                                                ]
                                                            }
                                                        },
                                                        "in": {
                                                            "$sum": [
                                                                "$$let_key_4.price",
                                                                "$$let_key_4.fee",
                                                            ]
                                                        },
                                                    }
                                                },
                                                {
                                                    "$subtract": [
                                                        0.0,
                                                        {
                                                            "$reduce": {
                                                                "input": "$discounts",
                                                                "initialValue": 0.0,
                                                                "in": {
                                                                    "$sum": [
                                                                        "$$value",
                                                                        "$$this.price",
                                                                        "$$this.fee",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                    ]
                                                },
                                                {
                                                    "$reduce": {
                                                        "input": "$order.contentLocations",
                                                        "initialValue": 0.0,
                                                        "in": {
                                                            "$let": {
                                                                "vars": {
                                                                    "let_key_6": {
                                                                        "$arrayElemAt": [
                                                                            {
                                                                                "$filter": {
                                                                                    "input": "$$this.vehicleTypesPricesFees",
                                                                                    "as": "key_5",
                                                                                    "cond": {
                                                                                        "$eq": [
                                                                                            "$$key_5.vehicleType",
                                                                                            "$$let_key_2",
                                                                                        ]
                                                                                    },
                                                                                }
                                                                            },
                                                                            0.0,
                                                                        ]
                                                                    }
                                                                },
                                                                "in": {
                                                                    "$sum": [
                                                                        "$$value",
                                                                        {
                                                                            "$multiply": [
                                                                                2.0,
                                                                                "$$this.tollPrice",
                                                                            ]
                                                                        },
                                                                        {
                                                                            "$multiply": [
                                                                                2.0,
                                                                                "$$this.tollFee",
                                                                            ]
                                                                        },
                                                                        "$$this.additionalPrice",
                                                                        "$$this.additionalFee",
                                                                        "$$this.entrancePrice",
                                                                        "$$this.entranceFee",
                                                                        "$$this.parkingPrice",
                                                                        "$$this.parkingFee",
                                                                        "$$this.waitingPrice",
                                                                        "$$this.waitingFee",
                                                                        "$$let_key_6.price",
                                                                        "$$let_key_6.fee",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                    }
                                                },
                                                {
                                                    "$reduce": {
                                                        "input": "$order.customLocations",
                                                        "initialValue": 0.0,
                                                        "in": {
                                                            "$let": {
                                                                "vars": {
                                                                    "let_key_8": {
                                                                        "$arrayElemAt": [
                                                                            {
                                                                                "$filter": {
                                                                                    "input": "$$this.vehicleTypesPricesFees",
                                                                                    "as": "key_7",
                                                                                    "cond": {
                                                                                        "$eq": [
                                                                                            "$$key_7.vehicleType",
                                                                                            "$$let_key_2",
                                                                                        ]
                                                                                    },
                                                                                }
                                                                            },
                                                                            0.0,
                                                                        ]
                                                                    }
                                                                },
                                                                "in": {
                                                                    "$sum": [
                                                                        "$$value",
                                                                        "$$this.waitingPrice",
                                                                        "$$this.waitingFee",
                                                                        "$$let_key_8.price",
                                                                        "$$let_key_8.fee",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                    }
                                                },
                                            ]
                                        },
                                    }
                                },
                            }
                        },
                    }
                }
            }
        },
        {
            "$addFields": {
                "price": {
                    "$let": {
                        "vars": {
                            "let_key_9": {
                                "$sum": [
                                    {"$ceil": {"$multiply": [2.0, "$order.tollPrice"]}},
                                    "$order.additionalPrice",
                                ]
                            }
                        },
                        "in": {
                            "$subtract": [
                                {
                                    "$reduce": {
                                        "input": "$order.vehicles",
                                        "initialValue": 0.0,
                                        "in": {
                                            "$let": {
                                                "vars": {"let_key_10": "$$this"},
                                                "in": {
                                                    "$sum": [
                                                        "$$value",
                                                        {
                                                            "$sum": [
                                                                "$$let_key_9",
                                                                {
                                                                    "$let": {
                                                                        "vars": {
                                                                            "let_key_12": {
                                                                                "$arrayElemAt": [
                                                                                    {
                                                                                        "$filter": {
                                                                                            "input": "$order.vehicleTypesPricesFees",
                                                                                            "as": "key_11",
                                                                                            "cond": {
                                                                                                "$eq": [
                                                                                                    "$$key_11.vehicleType",
                                                                                                    "$$let_key_10",
                                                                                                ]
                                                                                            },
                                                                                        }
                                                                                    },
                                                                                    0.0,
                                                                                ]
                                                                            }
                                                                        },
                                                                        "in": {
                                                                            "$sum": [
                                                                                "$$let_key_12.price"
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                {
                                                                    "$reduce": {
                                                                        "input": "$order.contentLocations",
                                                                        "initialValue": 0.0,
                                                                        "in": {
                                                                            "$let": {
                                                                                "vars": {
                                                                                    "let_key_14": {
                                                                                        "$arrayElemAt": [
                                                                                            {
                                                                                                "$filter": {
                                                                                                    "input": "$$this.vehicleTypesPricesFees",
                                                                                                    "as": "key_13",
                                                                                                    "cond": {
                                                                                                        "$eq": [
                                                                                                            "$$key_13.vehicleType",
                                                                                                            "$$let_key_10",
                                                                                                        ]
                                                                                                    },
                                                                                                }
                                                                                            },
                                                                                            0.0,
                                                                                        ]
                                                                                    }
                                                                                },
                                                                                "in": {
                                                                                    "$sum": [
                                                                                        "$$value",
                                                                                        {
                                                                                            "$ceil": {
                                                                                                "$multiply": [
                                                                                                    2.0,
                                                                                                    "$$this.tollPrice",
                                                                                                ]
                                                                                            }
                                                                                        },
                                                                                        "$$this.additionalPrice",
                                                                                        "$$this.entrancePrice",
                                                                                        "$$this.parkingPrice",
                                                                                        "$$this.waitingPrice",
                                                                                        "$$let_key_14.price",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                    }
                                                                },
                                                                {
                                                                    "$reduce": {
                                                                        "input": "$order.customLocations",
                                                                        "initialValue": 0.0,
                                                                        "in": {
                                                                            "$let": {
                                                                                "vars": {
                                                                                    "let_key_16": {
                                                                                        "$arrayElemAt": [
                                                                                            {
                                                                                                "$filter": {
                                                                                                    "input": "$$this.vehicleTypesPricesFees",
                                                                                                    "as": "key_15",
                                                                                                    "cond": {
                                                                                                        "$eq": [
                                                                                                            "$$key_15.vehicleType",
                                                                                                            "$$let_key_10",
                                                                                                        ]
                                                                                                    },
                                                                                                }
                                                                                            },
                                                                                            0.0,
                                                                                        ]
                                                                                    }
                                                                                },
                                                                                "in": {
                                                                                    "$sum": [
                                                                                        "$$value",
                                                                                        "$$this.waitingPrice",
                                                                                        "$$let_key_16.price",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                    }
                                                                },
                                                            ]
                                                        },
                                                    ]
                                                },
                                            }
                                        },
                                    }
                                },
                                {
                                    "$reduce": {
                                        "input": "$discounts",
                                        "initialValue": 0.0,
                                        "in": {"$sum": ["$$value", "$$this.price"]},
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {
            "$addFields": {
                "fee": {
                    "$let": {
                        "vars": {
                            "let_key_17": {
                                "$sum": [
                                    {"$multiply": [2.0, "$order.tollFee"]},
                                    "$order.additionalFee",
                                ]
                            }
                        },
                        "in": {
                            "$subtract": [
                                {
                                    "$reduce": {
                                        "input": "$order.vehicles",
                                        "initialValue": 0.0,
                                        "in": {
                                            "$let": {
                                                "vars": {"let_key_18": "$$this"},
                                                "in": {
                                                    "$sum": [
                                                        "$$value",
                                                        {
                                                            "$sum": [
                                                                "$$let_key_17",
                                                                {
                                                                    "$let": {
                                                                        "vars": {
                                                                            "let_key_20": {
                                                                                "$arrayElemAt": [
                                                                                    {
                                                                                        "$filter": {
                                                                                            "input": "$order.vehicleTypesPricesFees",
                                                                                            "as": "key_19",
                                                                                            "cond": {
                                                                                                "$eq": [
                                                                                                    "$$key_19.vehicleType",
                                                                                                    "$$let_key_18",
                                                                                                ]
                                                                                            },
                                                                                        }
                                                                                    },
                                                                                    0.0,
                                                                                ]
                                                                            }
                                                                        },
                                                                        "in": {
                                                                            "$sum": [
                                                                                "$$let_key_20.fee"
                                                                            ]
                                                                        },
                                                                    }
                                                                },
                                                                {
                                                                    "$reduce": {
                                                                        "input": "$order.contentLocations",
                                                                        "initialValue": 0.0,
                                                                        "in": {
                                                                            "$let": {
                                                                                "vars": {
                                                                                    "let_key_22": {
                                                                                        "$arrayElemAt": [
                                                                                            {
                                                                                                "$filter": {
                                                                                                    "input": "$$this.vehicleTypesPricesFees",
                                                                                                    "as": "key_21",
                                                                                                    "cond": {
                                                                                                        "$eq": [
                                                                                                            "$$key_21.vehicleType",
                                                                                                            "$$let_key_18",
                                                                                                        ]
                                                                                                    },
                                                                                                }
                                                                                            },
                                                                                            0.0,
                                                                                        ]
                                                                                    }
                                                                                },
                                                                                "in": {
                                                                                    "$sum": [
                                                                                        "$$value",
                                                                                        {
                                                                                            "$multiply": [
                                                                                                2.0,
                                                                                                "$$this.tollFee",
                                                                                            ]
                                                                                        },
                                                                                        "$$this.additionalFee",
                                                                                        "$$this.entranceFee",
                                                                                        "$$this.parkingFee",
                                                                                        "$$this.waitingFee",
                                                                                        "$$let_key_22.fee",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                    }
                                                                },
                                                                {
                                                                    "$reduce": {
                                                                        "input": "$order.customLocations",
                                                                        "initialValue": 0.0,
                                                                        "in": {
                                                                            "$let": {
                                                                                "vars": {
                                                                                    "let_key_24": {
                                                                                        "$arrayElemAt": [
                                                                                            {
                                                                                                "$filter": {
                                                                                                    "input": "$$this.vehicleTypesPricesFees",
                                                                                                    "as": "key_23",
                                                                                                    "cond": {
                                                                                                        "$eq": [
                                                                                                            "$$key_23.vehicleType",
                                                                                                            "$$let_key_18",
                                                                                                        ]
                                                                                                    },
                                                                                                }
                                                                                            },
                                                                                            0.0,
                                                                                        ]
                                                                                    }
                                                                                },
                                                                                "in": {
                                                                                    "$sum": [
                                                                                        "$$value",
                                                                                        "$$this.waitingFee",
                                                                                        "$$let_key_24.fee",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                    }
                                                                },
                                                            ]
                                                        },
                                                    ]
                                                },
                                            }
                                        },
                                    }
                                },
                                {
                                    "$reduce": {
                                        "input": "$discounts",
                                        "initialValue": 0.0,
                                        "in": {"$sum": ["$$value", "$$this.fee"]},
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {
            "$addFields": {
                "paymentFulfilledAt0": {
                    "$arrayElemAt": ["$paymentsFull.fulfilledAt", 0.0]
                },
                "paymentFulfilledAt1": {
                    "$arrayElemAt": ["$paymentsFull.fulfilledAt", 1.0]
                },
                "paymentFulfilledAt2": {
                    "$arrayElemAt": ["$paymentsFull.fulfilledAt", 2.0]
                },
                "paymentFulfilledAt3": {
                    "$arrayElemAt": ["$paymentsFull.fulfilledAt", 3.0]
                },
                "paymentFailedAt0": {"$arrayElemAt": ["$paymentsFull.failedAt", 0.0]},
                "paymentFailedAt1": {"$arrayElemAt": ["$paymentsFull.failedAt", 1.0]},
                "paymentFailedAt2": {"$arrayElemAt": ["$paymentsFull.failedAt", 2.0]},
                "paymentFailedAt3": {"$arrayElemAt": ["$paymentsFull.failedAt", 3.0]},
                "paymentChargedBackAt0": {
                    "$arrayElemAt": ["$paymentsFull.chargedBackAt", 0.0]
                },
                "paymentChargedBackAt1": {
                    "$arrayElemAt": ["$paymentsFull.chargedBackAt", 1.0]
                },
                "paymentChargedBackAt2": {
                    "$arrayElemAt": ["$paymentsFull.chargedBackAt", 2.0]
                },
                "paymentChargedBackAt3": {
                    "$arrayElemAt": ["$paymentsFull.chargedBackAt", 3.0]
                },
                "paymentAmount0": {"$arrayElemAt": ["$paymentsFull.amount", 0.0]},
                "paymentAmount1": {"$arrayElemAt": ["$paymentsFull.amount", 1.0]},
                "paymentAmount2": {"$arrayElemAt": ["$paymentsFull.amount", 2.0]},
                "paymentAmount3": {"$arrayElemAt": ["$paymentsFull.amount", 3.0]},
            }
        },
        {"$addFields": {"matchedDocument": {"$arrayElemAt": ["$matchedData", int(0)]}}},
        {
            "$addFields": {
                "travelDataDuration": "$matchedDocument.duration",
                "travelDataDistance": "$matchedDocument.distance",
                "customLocationsCount": {"$size": "$customLocations"},
                "contentLocationsCount": {"$size": "$contentLocations"},
                "isLite": {"$in": [int(100), "$vehicles"]},
            }
        },
        {"$unwind": {"path": "$payments", "preserveNullAndEmptyArrays": True}},
        {"$unwind": {"path": "$partnerCollection", "preserveNullAndEmptyArrays": True}},
        {
            "$unwind": {
                "path": "$originLocationCollection",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$destinationLocationCollection",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$originLocationCollection.countryId",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$destinationLocationCollection.countryId",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$destinationCountryCollection",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$originCountryCollection",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {"$unwind": {"path": "$originLocationCollection"}},
        {"$unwind": {"path": "$originLocationCollection.name"}},
        {"$unwind": {"path": "$destinationLocationCollection"}},
        {"$unwind": {"path": "$destinationLocationCollection.name"}},
        {"$unwind": {"path": "$requestHeader", "preserveNullAndEmptyArrays": True}},
        {
            "$unwind": {
                "path": "$requestHeader.client",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$unwind": {
                "path": "$requestHeader.userAgent",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {"$unwind": {"path": "$pricingCountry", "preserveNullAndEmptyArrays": True}},
        {"$unwind": {"path": "$userCollection", "preserveNullAndEmptyArrays": True}},
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
            "$addFields": {
                "vehiclesString": {
                    "$replaceAll": {
                        "input": "$vehiclesString",
                        "find": "0",
                        "replacement": "sedan",
                    }
                },
                "totalStopsCount": {
                    "$sum": ["$customLocationsCount", "$contentLocationsCount"]
                },
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
        {
            "$addFields": {
                "driverName0": {
                    "$concat": ["$driverFirstName0", " ", "$driverLastName0"]
                },
                "driverName1": {
                    "$concat": ["$driverFirstName1", " ", "$driverLastName1"]
                },
                "driverName2": {
                    "$concat": ["$driverFirstName2", " ", "$driverLastName2"]
                },
                "hasAdditionalStop": {
                    "$cond": {
                        "if": {"$gte": ["$totalStopsCount", int(1)]},
                        "then": True,
                        "else": False,
                    }
                },
                "orderStatusString": {"$toString": "$status"},
                "isPool": {"$toBool": "$type"},
                "paymentMethodString": {"$toString": "$paymentMethod"},
                "currencyString": {"$toString": "$pricingCurrency"},
            }
        },
        {
            "$addFields": {
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
                                                        {"$type": "$order.cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.confirmedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.draftedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.draftedAt", None]},
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
                                                        {"$type": "$order.cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$order.acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$order.acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.confirmedAt", None]},
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
                                                        {"$type": "$order.cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$order.declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$order.declinedAt", None]},
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
                                                        {"$type": "$order.cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$order.acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$order.acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$order.confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$order.confirmedAt", None]},
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
                                                        {"$type": "$order.cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$order.cancelledAt", None]},
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
                                                        {"$type": "$order.cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.confirmedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.draftedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.draftedAt", None]},
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
                                                        {"$type": "$order.cancelledAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.cancelledAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.declinedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.declinedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.acceptedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.acceptedAt", None]},
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$order.confirmedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$eq": ["$order.confirmedAt", None]},
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$ne": [
                                                        {"$type": "$order.draftedAt"},
                                                        "missing",
                                                    ]
                                                },
                                                {"$ne": ["$order.draftedAt", None]},
                                            ]
                                        },
                                    ]
                                },
                                "then": 6.0,
                            },
                        ],
                        "default": "not recognized",
                    }
                }
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
                        "replacement": "Partners + Travel Agents",
                    }
                }
            }
        },
        {
            "$addFields": {
                "currencyString": {
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
                "currencyString": {
                    "$replaceAll": {
                        "input": "$currencyString",
                        "find": "1",
                        "replacement": "USD",
                    }
                }
            }
        },
        {
            "$addFields": {
                "b2bMarginTotalPricePrice": {"$subtract": ["$totalPrice", "$price"]},
                "partnerFee": {"$multiply": ["$totalPrice", 0.1]},
                "travelAgentId": "$userCollection.travelAgent.agentId",
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
            "$addFields": {
                "b2bMarginPartnerFee": {
                    "$subtract": ["$b2bMarginTotalPricePrice", "$partnerFee"]
                }
            }
        },
        {
            "$addFields": {
                "b2bMargin": {
                    "$subtract": ["$b2bMarginPartnerFee", "$sumOfChargebacks"]
                },
                "userAgent": "$requestHeader.userAgent",
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
                "totalStopsCount": 1.0,
                "customerNote": "$partnerCollection.customerNote",
                "totalPrice": "$totalPrice",
                "isLite": 1.0,
                "hasAdditionalStop": 1.0,
                "travelDataDuration": 1.0,
                "travelDataDistance": 1.0,
                "passengersCount": 1.0,
                "vehiclesCount": 1.0,
                "additionalStopCount": 1.0,
                "partnerId": 1.0,
                "affiliatePartnerId": 1.0,
                "travelAgentOwnerId": 1.0,
                "rating1": 1.0,
                "rating2": 1.0,
                "rating3": 1.0,
                "rating4": 1.0,
                "rating5": 1.0,
                "price": "$price",
                "fee": "$fee",
                "discountsPrice": {
                    "$reduce": {
                        "input": "$discounts",
                        "initialValue": 0.0,
                        "in": {"$sum": ["$$value", "$$this.price"]},
                    }
                },
                "discountsFee": {
                    "$reduce": {
                        "input": "$discounts",
                        "initialValue": 0.0,
                        "in": {"$sum": ["$$value", "$$this.fee"]},
                    }
                },
                "pricingCountryISOCode": {"$ifNull": ["$pricingCountry.isoCode", ""]},
                "createdAt": "$createdAt",
                "departureAt": "$departureAt",
                "paymentMethod": "$paymentMethod",
                "pricingCurrency": "$pricingCurrency",
                "originCountry": "$originCountryCollection.englishName",
                "destinationCountry": "$destinationCountryCollection.englishName",
                "pricingCountryName": "$pricingCountry.englishName",
                "currencyRate": 1.0,
                "type": 1.0,
                "userId": "$order.userId",
                "sumOfSubsidies": 1.0,
                "route": {
                    "$concat": [
                        "$originLocationCollection.name",
                        " ",
                        "$destinationLocationCollection.name",
                    ]
                },
                "originLocation": "$originLocationCollection.name",
                "destinationLocation": "$destinationLocationCollection.name",
                "acceptedAt": 1.0,
                "declinedAt": 1.0,
                "confirmedAt": 1.0,
                "cancelledAt": 1.0,
                "paymentFulfilledAt0": 1.0,
                "paymentFulfilledAt1": 1.0,
                "paymentFulfilledAt2": 1.0,
                "paymentFulfilledAt3": 1.0,
                "paymentChargedBackAt0": 1.0,
                "paymentChargedBackAt1": 1.0,
                "paymentChargedBackAt2": 1.0,
                "paymentChargedBackAt3": 1.0,
                "paymentFailedAt0": 1.0,
                "paymentFailedAt1": 1.0,
                "paymentFailedAt2": 1.0,
                "paymentFailedAt3": 1.0,
                "paymentAmount0": 1.0,
                "paymentAmount1": 1.0,
                "paymentAmount2": 1.0,
                "paymentAmount3": 1.0,
                "isPaidOut": "$payments.isPaidOut",
                "driverName0": 1.0,
                "driverName1": 1.0,
                "driverName2": 1.0,
                "driver0Company": 1.0,
                "driver1Company": 1.0,
                "driver2Company": 1.0,
                "routeId": 1.0,
                "sumOfChargebacks": 1.0,
                "vehicleYearOfManufacture0": 1.0,
                "vehicleYearOfManufacture1": 1.0,
                "travelAgentId": 1.0,
                "vehicleYearOfManufacture2": 1.0,
                "vehicleModel0": 1.0,
                "vehicleModel1": 1.0,
                "vehicleModel2": 1.0,
                "vehicleMake0": 1.0,
                "vehicleMake1": 1.0,
                "vehicleMake2": 1.0,
                "isBusinessTrip": 1.0,
                "paymentType": 1.0,
                "driverRating": 1.0,
                "isPool": 1.0,
                "b2bMargin": 1.0,
                "b2bMarginTotalPricePrice": 1.0,
                "partnerFee": 1.0,
                "b2bMarginPartnerFee": 1.0,
                "userOS": 1.0,
                "userBrowser": 1.0,
                "createdBy": 1.0,
                "paymentMethodB2B": 1.0,
                "orderStatusCheck": "$status",
                "ipAddress": "$requestHeader.remoteAddress",
                "userAgent": 1.0,
                "requestHeaderClientName": "$requestHeader.client.name",
                "vehicleType": "$vehiclesString",
                "createdDepartureDiff": {
                    "$divide": [
                        {"$subtract": ["$departureAt", "$createdAt"]},
                        86400000.0,
                    ]
                },
                "orderStatus": "$orderStatusString",
                "currency": "$currencyString",
                "travelAgentApprovedAt": "$userCollection.travelAgent.approvedAt",
                "travelAgentCreatedAt": "$userCollection.travelAgent.createdAt",
            }
        },
    ]
