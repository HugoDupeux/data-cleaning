
CREATE OR REPLACE FUNCTION garantme.create_or_update_refound_credit_note(
	IN invoice_id integer,
	IN amount integer default -1
)
    RETURNS integer
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100

AS $BODY$


	DECLARE
		nb_invoice integer;
		invoice_amount integer;
		update_amount integer;
		nb_cn integer;
		ac_id integer;
        td timestamp with time zone;
        have_termination_fees_invoice integer;
	
	BEGIN   
			
		-- check if the invoice exist
		SELECT COUNT(*) INTO nb_invoice
		FROM garantme."ApplicationInvoice" 
		WHERE 
			"deletedAt" IS NULL
			AND id=invoice_id;
			
		IF nb_invoice = 0 THEN
			RAISE EXCEPTION 'The invoice % could not be found', invoice_id;
		END IF;


		--check if the invoice have a credit note
		SELECT COUNT(DISTINCT CN.id) INTO nb_cn
		FROM garantme."CreditNote" CN
		WHERE 
            CN."applicationInvoiceId"=invoice_id 
            AND CN."deletedAt" IS NULL;

		SELECT "amountWithVAT" INTO invoice_amount
		FROM garantme."ApplicationInvoice" 
		WHERE 
			"deletedAt" IS NULL
			AND id=invoice_id;

		-- if there is no amount in parameters (default value), affect the invoice amount
		IF amount = -1 THEN 
			update_amount := invoice_amount;
		ELSE 
			update_amount := amount;
		END IF;

        --check if the AC got a terminationDate
		SELECT "terminationDate" INTO td
        FROM garantme."ApartmentContract"
        WHERE 
            id = (
                SELECT "apartmentContractId"
                FROM garantme."ApplicationInvoice"
                WHERE id=invoice_id AND "deletedAt" IS NULL
            ) 
            AND "deletedAt" IS NULL;
            
        IF td IS NULL THEN
            RAISE EXCEPTION 'The AC does not have tD';
		END IF;
        
        -- check if the AC got a invoice termination fees
        SELECT COUNT(AI.id) INTO have_termination_fees_invoice
        FROM garantme."ApplicationInvoice" AI
        WHERE 
            AI."apartmentContractId" = (
                SELECT "apartmentContractId"
                FROM garantme."ApplicationInvoice"
                WHERE id=invoice_id AND "deletedAt" IS NULL
            )
            AND AI."paymentMotiveTypeId"=5
            AND AI."deletedAt" IS NULL
        ;

			
		-- if the CN does not exists, we create one with the specified amount fill in the parameters
		IF nb_cn = 0 THEN
			
			RAISE NOTICE 'Create the credit for invoice %', invoice_id;

			-- check if the amount if less than the incoice amount
			IF update_amount > invoice_amount THEN 
				RAISE EXCEPTION 'The amount is more than the invoice %', invoice_id;
			END IF;
            
			-- create the CN
			INSERT INTO "CreditNote" 
			(
				"amount", 
				"creditNoteCreationMotiveTypeId", 
				"applicationInvoiceId", 
				"apartmentContractId", 
				"applicationId", 
				"updatedAt", 
				"createdAt",
                "accountingDate",
                "creationTypeId"
			)
			VALUES 
			(
				update_amount, 
				1, 
				invoice_id, 
				(SELECT "apartmentContractId" FROM garantme."ApplicationInvoice" WHERE id=invoice_id AND "deletedAt" IS NULL), 
				(SELECT "applicationId" FROM garantme."ApplicationInvoice" WHERE id=invoice_id AND "deletedAt" IS NULL), 
				NOW(), 
				NOW(),
                td,
                6
			);

			-- Termination fees
            IF have_termination_fees_invoice=0 THEN 
                
                RAISE NOTICE 'Create invoice for termination fees';
            
			    INSERT INTO "ApplicationInvoice" (
			    	"amountWithoutVAT", 
			    	"amountWithVAT", 
			    	"amountToCharge", 
			    	"VAT", 
			    	"dueDate", 
			    	"accountingDate", 
			    	"creationTypeId", 
			    	"paymentMotiveTypeId", 
			    	"apartmentContractId", 
			    	"applicationId", 
			    	"updatedAt", 
			    	"createdAt")
			    VALUES (
				    LEAST(7000,update_amount), 
				    LEAST(7000,update_amount), 
				    LEAST(7000,update_amount), 
				    0, 
				    td + interval '1 month', 
				    td, 
				    6, 
				    5, 
				    (SELECT "apartmentContractId" FROM garantme."ApplicationInvoice" WHERE id=invoice_id AND "deletedAt" IS NULL), 
				    (SELECT "applicationId" FROM garantme."ApplicationInvoice" WHERE id=invoice_id AND "deletedAt" IS NULL), 
				    NOW(), 
				    NOW()
			    );
            END IF;



		-- if the CN exists, we update it with the full amount of the invoice
		ELSIF nb_cn = 1 THEN

			RAISE NOTICE 'Update the credit for invoice %', invoice_id;

			UPDATE "CreditNote"
			SET 
				"updatedAt" = NOW(),
				"creditNoteCreationMotiveTypeId"=1,
				"amount" = update_amount
			WHERE 
				"applicationInvoiceId"=invoice_id
				AND "deletedAt" IS NULL;

		--if there is more than on CN attached to the CN, do nothing because is too complexe
		ELSE
			RAISE NOTICE 'The invoice % have mutliple CN: skip', invoice_id;
		END IF;
		
		RETURN 0;
    END

$BODY$;
