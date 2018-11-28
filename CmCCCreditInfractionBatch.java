package com.splwg.cm.domain.batch.creditInfraction;

/*
 * Revision History
 * Date				Author					Comments
 * ----------		---------------			-----------------------------
 * 2018-04-06		whittm2					Convert COBOL to Java 
 *                                          
 */
import java.math.BigInteger;
import java.util.List;
import java.lang.Math;

import com.splwg.base.api.ListFilter;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.DefaultJobWork;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.ccb.domain.admin.customerContactClass.entity.CustomerContactClass_Id;
import com.splwg.ccb.domain.admin.customerContactType.entity.CustomerContactType;
import com.splwg.ccb.domain.admin.customerContactType.entity.CustomerContactType_Id;
import com.splwg.ccb.domain.admin.letterTemplate.entity.LetterTemplate_Id;
import com.splwg.ccb.domain.common.installation.entity.CcbInstallation;
import com.splwg.ccb.domain.common.installation.entity.CcbInstallation_Id;
import com.splwg.ccb.domain.customerinfo.account.entity.Account;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountPerson;
import com.splwg.ccb.domain.customerinfo.account.entity.Account_Id;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContact_DTO;
import com.splwg.ccb.domain.customerinfo.person.entity.Person;
import com.splwg.ccb.domain.customerinfo.person.entity.Person_Id;
import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;


	/**
	* @author whittm2
	*
	@BatchJob (multiThreaded = false, 
	 *                      rerunnable = false, 
	 *                      modules = {},
					softParameters = { @BatchJobSoftParameter (name = CR_LO, required = true, type = string)
				                     , @BatchJobSoftParameter (name = CR_HI, required = true, type = string)
	 *                               , @BatchJobSoftParameter (name = CC_CL_CD, type = string)
	 *                               , @BatchJobSoftParameter (name = CC_TYPE_CD, type = string)})
	*/

public class CmCCCreditInfractionBatch extends CmCCCreditInfractionBatch_Gen {
	private String ccClCd;
    private String ccTyCd ;
    private String CR_LO;
    private String CR_HI;
	private String SPECIAL_ROLE_FLG = "WO";
	private String SA_STATUS_FLG = "20";
	private long l_CR_LO = 0L;
	private long l_CR_HI = 0L;
	private static LetterTemplate_Id ltrTmplId = null;

    private static final Logger _logger = LoggerFactory.getLogger(CmCCCreditInfractionBatch.class);	
    
      public ThreadWorkUnit createWorkUnit(QueryResultRow row) {
            
            ThreadWorkUnit threadUnit = new ThreadWorkUnit();
            threadUnit.setPrimaryId((Account_Id)row.getId("ACCT_ID", Account.class));
            threadUnit.addSupplementalData("SUM_CR_RATING", row.getInteger("SUM_CR_RATING"));            
            
            return threadUnit;
      } // end createWorkUnit method
      
      @Override
      public void validateSoftParameters(boolean isNewRun) {
    	ccClCd = this.getParameters().getCC_CL_CD();
  	    ccTyCd = this.getParameters().getCC_TYPE_CD();
  	    CR_LO = this.getParameters().getCR_LO();
  	    CR_HI = this.getParameters().getCR_HI();
          
  		//Check for null input parms Cd

  		if (isEmptyOrNull(ccClCd)) {
  			ErrorRoutine("CC_CL_CD"," is empty ", "-Required parameter");
  		}
  		if (isEmptyOrNull(ccTyCd)) {
  			ErrorRoutine("CC_TYPE_CD"," is empty ", "-Required parameter");
  		}
  		
  		if (isEmptyOrNull(CR_LO)) {
  			ErrorRoutine("CR_LO"," is empty ", "-Required parameter");
  		} else {
  			// Check numeric validity of CR_LO   			
  			try {
  				l_CR_LO = Long.parseLong(CR_LO.trim());
  			}
  			catch (NumberFormatException nfe) {
  				ErrorRoutine("CR_LO","is Not a valid number","");
  			} // end catch
  		} // end if CR_LO / else stmt 
  		
  		if (isEmptyOrNull(CR_HI)) {
  			ErrorRoutine("CR_HI"," is empty ", "-Required parameter");
  		} else {
  			// Check numeric validity of CR_HI    			
  			try {
  				l_CR_HI = Long.parseLong(CR_HI.trim());
  			}
  			catch (NumberFormatException nfe) {
  				ErrorRoutine("CR_HI","is Not a valid number","");
  			} // end catch
  		} // end if CR_HI / else stmt   
  		
  		if (l_CR_HI < l_CR_LO) {
				//_logger.info("\n\n\nCR_HI cannot be less than CR_LO \n\n");
				ErrorRoutine("CR_HI","Cannot be less than"," CR_LO ");
  		}

  		// Check validity of CC_CL_CD input parm
          CustomerContactClass_Id ccClId = new CustomerContactClass_Id(ccClCd);
          
	  	if (!ccClId.isValid()) {
  			//_logger.info("\n\n\nYour CC_CL_CD is not valid:"+  ccClCd+ "\n\n");
  			ErrorRoutine("CC_CL_CD", " is Invalid ", " Value Passed: " + ccClCd);
  		}
  		
  		//Check validity of CC_TYPE_CD input parm
          CustomerContactType_Id ccTyId  = new CustomerContactType_Id(ccClId,ccTyCd);
          CustomerContactType ccTy = ccTyId.getEntity();

  		if (isNull(ccTy)) {
  			//_logger.info("\n\n\nYour CC_TYPE_CD is not valid:"+  ccTyCd + "\n\n");
  			ErrorRoutine("CC_TYPE_CD"," is Invalid ", " Value Passed: " + ccTyCd);
  		} else {  
  			
  			//if CC_TYPE_CD select returns row, check the LTR_TMPL_CD -- cannot be null or space
  			if (isNull(ccTy.getLetterTemplateId())){
  				//_logger.info("\n\n\nYour CC_TYPE_CD is not valid--LTR_TMPL_CD is missing:"+  ccTyCd + "\n\n");
  				ErrorRoutine("CC_TYPE_CD"," is Missing LTR_TMPL_CD ", " CC_TYPE_CD Value Passed: " + ccTyCd);
  			} else {
  	          ltrTmplId = ccTy.getLetterTemplateId();  //Letter Template code will be used to create CC
  			}
  			
  		}   //end if ccTy is null/ else stmt     
      } //end validateSoftParameters method
      
      @SuppressWarnings({ "unchecked", "rawtypes" })
      public JobWork getJobWork() {
    	  // Get Credit Rating query 	  
		  DateTime START_DT = getProcessDateTime();
		  DateTime END_DT = getProcessDateTime();
		  /*
		   *  gets the maximum credits available for a account credit rating
		   */
		  
			CcbInstallation_Id ccbId = new CcbInstallation_Id("11111");		// Install_Opt_Id = 11111
			CcbInstallation ccbIns = ccbId.getEntity();	
			BigInteger CR_RATING_PTS = ccbIns.getBeginningCreditRating();
	      	_logger.info("\n\n\n Starting Credit Rating points "  + CR_RATING_PTS.toString() + "  \n\n");		
		  		
		 /*
		  * This query obtains an account list whose accounts have received a credit deduction
		  * on processing date and 
		  * putting the total account credit points between incoming LO/HI parms (849 and 999). 
		  *  
		  */
		  		
		  PreparedStatement stmt =  createPreparedStatement("" 
				+ " SELECT "
				+ "   HI.ACCT_ID, "
				+ "   SUM(HI.CR_RATING_PTS)+ :CR_RATING_PTS " // From query that obtained START-CR-RATING_PTS -
				+ "         AS SUM_CR_RATING "				
				+ " FROM "
				+ "    CI_CR_RAT_HIST HI, "
				+ "    (SELECT "
				+ "        SA.ACCT_ID "
				+ "     FROM "
				+ "        CI_SA SA, "
				+ "        CI_SA_TYPE SAT "
				+ "     WHERE "
				+ "             SA.SA_STATUS_FLG = :SA_STATUS_FLG "  //SA Status Active - Numeric Flag 20 
				+ "        AND SA.CIS_DIVISION  = SAT.CIS_DIVISION "
				+ "       AND SA.SA_TYPE_CD = SAT.SA_TYPE_CD "
				+ "        AND NOT SAT.SPECIAL_ROLE_FLG  = :SPECIAL_ROLE_FLG   " //SET TO 'WO' -
				+ "     GROUP BY "
				+ "        SA.ACCT_ID) SA, "
				+ "    (SELECT "
				+ "        ACCT_ID, "
				+ "        MAX(CR_RATING_PTS) "
				+ "     FROM "
				+ "        CI_CR_RAT_HIST "
				+ "     WHERE "
				+ "             CR_RATING_PTS < 0 "
				+ "        AND trunc(START_DT) = trunc(:START_DT)  " //S-ACCTCR -- Process-dt
				+ "     GROUP BY "
				+ "        ACCT_ID "
				+ "     HAVING "
				+ "        MAX(CR_RATING_PTS) < 0) LT "
				+ " WHERE "
				+ "    HI.START_DT >= TO_DATE('2003-06-01','YYYY-MM-DD')  "  //date intentionally hardcoded from previous ccb conversion
				+ "   AND HI.START_DT <= trunc(:START_DT)   " //S-ACCTCR  -- Process-dt
				+ "   AND (HI.END_DT >= trunc(:END_DT)   "   //S-ACCTCR  -- Process-dt
				+ "          OR HI.END_DT IS NULL) "  
				+ "    AND HI.ACCT_ID = SA.ACCT_ID "
				+ "    AND HI.ACCT_ID = LT.ACCT_ID "
				+ " GROUP BY "
				+ "    HI.ACCT_ID "
				+ " HAVING "
				+ "       SUM(HI.CR_RATING_PTS) + :CR_RATING_PTS  "  //S-ACCTCR  -- From query that obtained START-CR-RATING_PTS
				+ "         BETWEEN :CR_LO  "  //INPUT PARM
				+ "                  AND :CR_HI  " //INPUT PARM
				+ "    AND trunc(MAX(HI.START_DT)) = trunc(:START_DT)  "	//S-ACCTCR	  -- Process-dt
				+ "","");
	  			stmt.bindString("SA_STATUS_FLG",  SA_STATUS_FLG, "SA_STATUS_FLG"); 
	  			stmt.bindString("SPECIAL_ROLE_FLG",  SPECIAL_ROLE_FLG, "SPECIAL_ROLE_FLG");
	  			stmt.bindDateTime("START_DT", START_DT);
				stmt.bindDateTime("END_DT", END_DT);    	  
	  			stmt.bindBigInteger("CR_RATING_PTS", CR_RATING_PTS);
	  			stmt.bindBigInteger("CR_LO", BigInteger.valueOf(l_CR_LO));  			
	  			stmt.bindBigInteger("CR_HI", BigInteger.valueOf(l_CR_HI));
	
	      QueryIterator qryIter = stmt.iterate();
	      DefaultJobWork wrk = createJobWorkForQueryIterator(qryIter, this);
	      stmt.close();    	  
	      return wrk;
      } // end JobWork method
      
      public Class<CmCCCreditInfractionBatchWorker> getThreadWorkerClass() {
          return CmCCCreditInfractionBatchWorker.class;
    } // end getThreadWorkerClass() method
      
      
    	private void ErrorRoutine(String fieldName, String message1, String message2) {
    		
            ServerMessage srvMsg = new ServerMessage();
            MessageParameters messageParms = new MessageParameters();
            srvMsg.setCategory(BigInteger.valueOf(90000));
            srvMsg.setNumber(BigInteger.valueOf(1));
            srvMsg.setMessageParameters(messageParms);
            srvMsg.setProgramName("CmCCCreditInfractionBatch.java");
            srvMsg.setLongDescription(fieldName + message1 + message2);
    		_logger.error(srvMsg.getMessageText());
    		this.addError(srvMsg); //terminates program
    		return;
    		
    	} //end ErrorRoutine method 
    	
        public static class CmCCCreditInfractionBatchWorker extends CmCCCreditInfractionBatchWorker_Gen {
      	  
      	    private DateTime EFFDT = getProcessDateTime(); 
      	    
            public ThreadExecutionStrategy createExecutionStrategy() {
                //return new com.splwg.base.api.batch.SingleTransactionStrategy(this);
                return new CommitEveryUnitStrategy(this);
            } // end createExecutionStrategy method  
            
            
            public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {
                Account_Id acct_Id = (Account_Id)unit.getPrimaryId();
                String sAcct_Id = acct_Id.getIdValue();                
                Account acct = acct_Id.getEntity();        
                long SUM_CR_RATING = 0L;
                
          		//_logger.info("\n\n\n AcctID "  + sAcct_Id + " Selected for work "); 
          		
          		BigInteger biSUM_CR_RATING =  (BigInteger) unit.getSupplementallData("SUM_CR_RATING");

          		SUM_CR_RATING = biSUM_CR_RATING.longValue();

          		/*	
          		 * Check for credit Rating deductions
          		 *	If this count > 0 then   		       		
          		 *	using the sAcct_id, get all account persons.
          		 */          		
 
          		if (crDeduction(sAcct_Id, EFFDT, EFFDT)) {
          			
          			//List<AccountPerson> allPers = (List<AccountPerson>) acct.getPersons();
	                ListFilter<AccountPerson> finRespPersFilter  =  acct.getPersons().createFilter("WHERE this.isFinanciallyResponsible = :finRespSwitch","finRespPersFilter");
	                finRespPersFilter.bindBoolean("finRespSwitch", Bool.TRUE);
	                List<AccountPerson> acctPersList = finRespPersFilter.list();
	                
              		/*	
	              	 *	For Financially responsible person
	              	 *	Check if CC and CC NSF exists
	              	 *	If no CC or CC NSF then Get credit points deducted
	              	 *  and Create CC
	                 */

	                if (!isNull(acctPersList) && !acctPersList.isEmpty()) {
	                	
	                	for (int i = 0; i < acctPersList.size(); i++) {
	                		
	                		AccountPerson acctPer = acctPersList.get(i);
	                		
	                		Person pers = (Person) acctPer.fetchIdPerson();
	                		Person_Id pers_Id = pers.getId();	                		
	                		String sPer_Id = pers_Id.getIdValue();               		

	                		/*
	                		 * If Customer Contact record already exists, another will not be created, 
	                		 * go to next financially responsible person
	                		 * or next account.
	                		 */
	                          if (!ccExists(sPer_Id,EFFDT,EFFDT)) {
                		  /*
	                      		   * Get the sum of credit points that were deducted from the acount
	                      		   * credit rating.  This number will be used in the LongDescr field
	                      		   * message
	                      		   */
                      		  
		                       	   int crRatingPoints = 0;
		                    	   DateTime crRatingEndDt = null;
		                    	   PreparedStatement stmtPts =  createPreparedStatement("" 
		                   			 + " SELECT END_DT, SUM(CR_RATING_PTS) CR_RATING_PTS"
		                   			 + " 	FROM CI_CR_RAT_HIST "
		                   			 + " WHERE CR_RATING_PTS < 0 "
		                   			 + " AND  trunc(START_DT) = trunc(:START_DT)  " //from proces-dt
		                   			 + " AND (   trunc(END_DT) >= trunc(:END_DT)  "  //from process-dt
		                   		     + "     OR END_DT IS NULL) "
		                   		     + " AND ACCT_ID = :ACCT_ID  " // acct-id from work query
		                   		     + " GROUP BY END_DT "
		                    		 + "","");		
		                    		stmtPts.bindString("ACCT_ID",  sAcct_Id, "ACCT_ID"); 
		                    		stmtPts.bindDateTime("START_DT", EFFDT);
		                    		stmtPts.bindDateTime("END_DT", EFFDT);		
		                    		SQLResultRow rowPts = stmtPts.firstRow();
	
		                    		if (!isNull(rowPts)) {        			
		                    			crRatingEndDt = (rowPts.getDateTime("END_DT"));
		                    			try {
		                    				crRatingPoints = Integer.parseInt(rowPts.get("CR_RATING_PTS").toString());
		                    		        stmtPts.close();    
		                    			}
		                    			catch (NumberFormatException nfe) {
		                    		         stmtPts.close(); 
		                    		         nfe.printStackTrace();
		                    				//_logger.info("\n\n\nNumberFormatException: " + nfe.getMessage() + "\n\n");
		                    			} //end try catch for parse
	
		                    		} 	else {
		                		         stmtPts.close();    
		                    			//_logger.info("\n\n\n No record returned from Select COUNT: ERROR \n\n");			
	
		                    		} // end if row is null /else stmt  	                      		  
	                      		  
	                      		  
	                      		 //set up CC Class Id and CC Type ID
	        	                CustomerContactClass_Id cccId = new CustomerContactClass_Id(this.getParameters().getCC_CL_CD());
	        	                CustomerContactType_Id ccTyId  = new CustomerContactType_Id(cccId,this.getParameters().getCC_TYPE_CD()); 
  	                
	
	        	                // Create the CC     
	        	        		CustomerContact_DTO ccNew = new CustomerContact_DTO();
	        	        		ccNew.setPersonId(pers_Id);
	        	                ccNew.setUserId(getActiveContextUser().getId());        		
	        	        		ccNew.setContactDateTime(getProcessDateTime());
	        	        		ccNew.setCustomerContactTypeId(ccTyId); 
	        	        		ccNew.setLetterTemplateId(ltrTmplId);  //this value is from the validation for CC_TYPE_CD Letter template code lookup

		
	        	        		/*
	        	        		 * COBOL program Rating Points and Sum Rating points were padded fields.  Additionally the Rating points
	        	        		 * which is a negative number had the sign trailing. 
	        	        		 * The Rating End Date can be null and need to allow blank in the message.
	        	        		 * 
	        	        		 */
	        	        		
	        	        		DateFormat df = new DateFormat("yyyy-MM-dd");	        	        		
	        	        		String endDT = this.isNull(crRatingEndDt) ? " " : df.format(crRatingEndDt); 
	        	        		String signCR = (crRatingPoints < 0) ? "-" : " ";
	        	        		String signSCR = (SUM_CR_RATING < 0) ? "-" : " ";	        	        		

	        	        		ccNew.setLongDescription("Your Internal Credit Score has been affected by " 
	        	        								+ String.format("%1$6s", Integer.toString(Math.abs(crRatingPoints))) + signCR
	        	        								+ " points and your new credit score is " 
	        	        								+ String.format("%1$6s", Long.toString(Math.abs(SUM_CR_RATING))) + signSCR
	        	        								+ " that will expire on " + endDT) ;

	        	        		ccNew.newEntity();
	        	                  
	        	              	_logger.info("\n\n\n Created CC "  + sAcct_Id + "  " + ccNew.getId().toString() + "  \n\n");
	
	                      	} // end if !ccExists stmt	                	
	                	
	                	} // end for loop accPersList	                	
	                	
	                } // end if acctPersList is null or empty
          		} //end if crDeduction	
                  
                return true;
                  
            } // end executeWorkUnit method  

            private boolean ccExists(String sPerId, DateTime START_DT, DateTime END_DT) {
        		boolean ccExists = false;
        		boolean nsfExists = false;
        		/*
        		 * Gets Customer Contact record counts for the date period given or calculated
        		 * for CC Class, DEPWARN Type
        		 */
        		
        		PreparedStatement stmtCC =  createPreparedStatement("" 
        			+ " SELECT   "
        			+ " COUNT(*) CCCOUNT   "
        			+ " FROM    "
        			+ "     CI_CC   "
        			+ " WHERE   "
        			+ "     PER_ID = :PER_ID   " // current person of person collection
        			+ " AND CC_CL_CD = :CC_CL_CD   " //input parm
        			+ " AND CC_TYPE_CD = :CC_TYPE_CD   " //input parm
        			+ " AND TRUNC(CC_DTTM) BETWEEN trunc(:START_DT) AND trunc(:END_DT)   "	//start-dt is process-dt -1, end date is process-dt	
        			 + "","");		
        		stmtCC.bindString("PER_ID",  sPerId, "PER_ID"); 
        		stmtCC.bindString("CC_CL_CD",  this.getParameters().getCC_CL_CD(), "CC_CL_CD");
        		stmtCC.bindString("CC_TYPE_CD", this.getParameters().getCC_TYPE_CD(), "CC_TYPE_CD");
        		stmtCC.bindDateTime("START_DT", START_DT.addDays(-1));
        		stmtCC.bindDateTime("END_DT", END_DT);		
        		SQLResultRow rowCC = stmtCC.firstRow();
        		int ccCount = 0;
        		if (!isNull(rowCC)) {        			
        		
        			try {
        				ccCount = Integer.parseInt(rowCC.get("CCCOUNT").toString());
        		        stmtCC.close();    
        			}
        			catch (NumberFormatException nfe) {
        		         stmtCC.close();
        		         nfe.printStackTrace();
        				//_logger.info("\n\n\nNumberFormatException: " + nfe.getMessage() + "\n\n");
        			} //end try catch for parse

        			ccExists = (ccCount > 0) ? true : false;

        		} 	else {
    		         stmtCC.close();    
        			//_logger.info("\n\n\n No record returned from Select COUNT: ERROR \n\n");			

        		} // end if row is null /else stmt
        		
        		/*
        		 * Gets Customer Contact record counts for the date period given 
        		 * for CC records where the LTR_TMPL_CD contains NSF.
        		 */        		
        		PreparedStatement stmtNSF =  createPreparedStatement("" 
            			+ " SELECT   "
            			+ " COUNT(*) CCCOUNT   "
            			+ " FROM    "
            			+ "     CI_CC   "
            			+ " WHERE   "
            			+ "     PER_ID = :PER_ID   " // per-id from current person of person collection
            			+ " AND LTR_TMPL_CD like '%NSF%'  "  //any NSF letter that may have been sent for this PER_ID
            			+ " AND TRUNC(CC_DTTM) = trunc(:START_DT)   "	// start-dt is process dt	
            			+ "","");		
            		stmtNSF.bindString("PER_ID",  sPerId, "PER_ID"); 
            		stmtNSF.bindDateTime("START_DT", START_DT);
            		SQLResultRow rowNSF = stmtNSF.firstRow();
            		
            		int nsfCount = 0;
            		if (!isNull(rowNSF)) {        			
            		
            			try {
            				nsfCount = Integer.parseInt(rowNSF.get("CCCOUNT").toString());
            		        stmtNSF.close();    
            			}
            			catch (NumberFormatException nfe) {
            		         stmtNSF.close();    
            		         nfe.printStackTrace();
            				//_logger.info("\n\n\nNumberFormatException: " + nfe.getMessage() + "\n\n");
            			} //end try catch for parse

            			nsfExists = (nsfCount > 0) ? true : false;

            		} 	else {
        		         stmtNSF.close();    
            			//_logger.info("\n\n\n No record returned from Select COUNT: ERROR \n\n");			

            		} // end if row is null /else stmt
 
        		/*
            	 * Returning this condition indicates if there is a customer contact record
            	 * ( CC of DEPWARN or NSF) that has already been created for this person.  
            	 * 
            	 */
        		boolean itExists = (nsfExists || ccExists) ? true : false;
        		
        		
        		return itExists;
        	} // end ccExists method   
            
            private boolean crDeduction(String sAcctId, DateTime START_DT, DateTime END_DT) {
        		boolean itExists = false;
        		
        		/*
        		 * This query pulls the number of credit rating history records on file for
        		 * the date period given
        		 */
        		PreparedStatement stmtCRD =  createPreparedStatement("" 
    				 + " SELECT "
    				 + " COUNT(*) CRD_COUNT  "
        			 + " FROM  "
        			 + "    CI_CR_RAT_HIST  "
        			 + " WHERE  "
        			 + "         CR_RATING_PTS < 0  "
        			 + "     AND trunc(START_DT) = trunc(:START_DT)   "  // from process-dt
        			 + "     AND (trunc(END_DT) >= trunc(:END_DT)   " // from process-dt
        			 + "             OR END_DT IS NULL)  "
        			 + "     AND ACCT_ID = :ACCT_ID   "	// acct-id from work query
        			 + "","");		
        		stmtCRD.bindString("ACCT_ID",  sAcctId, "ACCT_ID"); 
        		stmtCRD.bindDateTime("START_DT", START_DT);
        		stmtCRD.bindDateTime("END_DT", END_DT);		
        		SQLResultRow rowCRD = stmtCRD.firstRow();
        		int crdCount = 0;
        		if (!isNull(rowCRD)) {        			
        		
        			try {
        				crdCount = Integer.parseInt(rowCRD.get("CRD_COUNT").toString());
        		        stmtCRD.close();    
        			}
        			catch (NumberFormatException nfe) {
        		         stmtCRD.close();    
        		         nfe.printStackTrace();
        				//_logger.info("\n\n\nNumberFormatException: " + nfe.getMessage() + "\n\n");
        			} //end try catch for parse

        			itExists = (crdCount > 0) ? true : false;

        		} 	else {
    		         stmtCRD.close();    
        			//_logger.info("\n\n\n No record returned from Select COUNT: ERROR \n\n");			

        		} // end if row is null /else stmt
        		
        		/*
        		 * Returning this condition indicates whether there are credit deduction
        		 * points for process date
        		 */
        		return itExists;
        	} // end ccDeduction method  
       
        } // end CmCCCreditInfractionBatchWorker 
        

	} //end CmCCCreditInfractionBatch class    	


