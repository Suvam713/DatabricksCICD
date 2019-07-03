// Databricks notebook source
//read the source from RAW JSON layer
val myDF=spark.read.format("json").load("/mnt/adlsedhdev/EDH/Timberline/Supplier/RawCDM/Data/9999-12-31")
myDF.count

// COMMAND ----------

myDF.printSchema

// COMMAND ----------

//val nDF=myDF.withColumn("x_Contacts",explode($"VCONTACT")).drop($"VCONTACT")

// COMMAND ----------

//nDF.printSchema

// COMMAND ----------

case class x_Contacts(x_FirstName: String, x_LastName: String, x_NumberType: String, x_Number: String)

// COMMAND ----------

//transform the json to the cleanse format
//Fuzzy logic need to be applied when generating "x_AddressContacts" attribute
import org.apache.spark.sql.functions._
val newDF=myDF.select(struct(coalesce($"VENDOR",lit("")) as "x_SourceVendorID",
                             coalesce($"VNAME",lit("")) as "Name",
                             lit("SPEND_AUTHORIZED") as "x_BusinessRelationship", 
                             lit("N") as "x_OneTimeSupplierFlag",
                             lit("N") as "x_BusinessClassificationNotApplicableFlag",
                             coalesce($"VTYPE",lit("")) as "VendorOrganizationType", 
                             coalesce($"VFEDID",lit("")) as "TaxID",
                             struct(lit("") as "x_ClassificationLookupCode",
                                    lit("") as "x_SubClassification",
                                    lit("") as "x_CertifyingAgencyName",
                                    lit("TRUE") as "x_CreateCertifyingAgencyFlag"
                             ) as "x_BusinessClassifications", 
                             struct(lit("") as "ID") as "Identifiers",
                             struct(coalesce($"VADDR1",lit("")) as "AddressLine1", 
                                    coalesce($"VADDR2",lit("")) as "AddressLine2", 
                                    coalesce($"VCITY",lit("")) as "City",
                                    coalesce($"VSTATE",lit("")) as "StateProvince",
                                    coalesce((when(length($"VSTATE")===2, lit("United States")).otherwise($"VSTATE")),lit("")) as "Country",
                                    coalesce($"VZIP",lit("")) as "PostalCode",
                                    lit("MAIN-PURCH") as "AddressType",
                                    lit("MGM_US") as "x_ProcurementBusinessUnitName",
                                    lit("MAIN-PURCH") as "x_VendorSiteCode",
                                    lit("N") as "x_RFQOnlySiteFlag",
                                    lit("N") as "x_PurchasingSiteFlag",
                                    lit("N") as "x_PCardSiteFlag",
                                    lit("Y") as "x_PaySiteFlag",
                                    lit("Y") as "x_PrimaryPaySiteFlag",
                                    lit("Y") as "x_TaxReportingSiteFlag",
                                    lit("MGM_US") as "x_BusinessUnitName",
                                    lit("MGM_US") as "x_BillToBusinessUnitName",
                                    array(
                                      struct(lit("Phone") as "x_NumberType",
                                             lit("-") as "x_NumberCountryCode",
                                             lit("-") as "x_NumberAreaCode",
                                             coalesce($"VPHONE",lit("")) as "x_Number"
                                      ),
                                      struct(lit("Fax") as "x_NumberType",
                                             lit("-") as "x_NumberCountryCode",
                                             lit("-") as "x_NumberAreaCode",
                                             coalesce($"VFAX",lit("")) as "x_Number"
                                      )
                                    ) as "x_AddressContacts"
                             ) as "Addresses", 
                             struct(coalesce($"VSTATE" ,lit("")) as "x_TaxPayerCountry",
                                    coalesce($"VTIDNAM",lit("")) as "x_TaxReportingName",
                                    coalesce($"VSCNTIN",lit("")) as "x_SecondTINNotice",
                                    coalesce($"V1099",lit("")) as "x_FederalIncomeTaxType",
                                    lit("Y") as "x_FederalReportableFlag",
                                    coalesce((when(regexp_replace($"VFEDID","(\\d+)","n")==="n-n-n","Individual").when(regexp_replace($"VFEDID","(\\d+)","n")==="n-n","Corporation").otherwise("N/A")),lit("")) as "x_TaxOrganizationType"
                             ) as "x_TaxInformation",
                             struct(coalesce($"VSTBDTL",lit("")) as "x_PaymentReasonCode",
                                    lit("") as "x_TSP",
                                    lit("") as "x_NVSalesCertification",
                                    lit("") as "x_MSSalesCertification",
                                    coalesce($"VOAMT",lit("")) as "x_OutstandingAmount", 
                                    coalesce($"VRETAIN",lit("")) as "x_RetainageHeld"
                                 ) as "x_Other",
                             array(
                              struct(coalesce(split(lit($"VCONTACT"(0).getItem("VCNAME"))," ").getItem(0),lit("")) as "x_FirstName",
                                   coalesce(split(lit($"VCONTACT"(0).getItem("VCNAME"))," ").getItem(1),lit("")) as "x_LastName",
                                   lit("") as "x_NumberType",
                                   coalesce($"VCONTACT"(0).getItem("VCTELE"),lit("")) as "x_Number"
                               ),
                             struct(coalesce(split(lit($"VCONTACT"(1).getItem("VCNAME"))," ").getItem(0),lit("")) as "x_FirstName",
                                     coalesce(split(lit($"VCONTACT"(1).getItem("VCNAME"))," ").getItem(1),lit("")) as "x_LastName",
                                     lit("") as "x_NumberType",
                                     coalesce($"VCONTACT"(1).getItem("VCTELE"),lit("")) as "x_Number"
                               )
                             ) as "x_contacts"
                           )as "Organization") 

// COMMAND ----------

newDF.printSchema

// COMMAND ----------

//Store in ADLS
newDF.write.mode("overwrite").format("com.databricks.spark.json").json("/mnt/adlsedhdev/EDH/Timberline/Supplier/Standard/interim/2018-11-05")

// COMMAND ----------

