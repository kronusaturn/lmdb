(in-package :cl-user)
(defpackage lmdb
  (:use :cl)
  (:shadow :get)
  (:export
   :*database-class*
   :*environment-class*
   :*transaction-class*
   :*transaction*
   :abort-transaction
   :begin-transaction
   :close-cursor
   :close-database
   :close-environment
   :commit-transaction
   :condition-environment
   :condition-name
   :cursor
   :cursor-get
   :database
   :database-maximum-count
   :database-not-found
   :del
   :do-pairs
   :drop-database
   :ensure-open-database
   :ensure-open-environment
   :environment
   :environment-info
   :environment-statistics
   :get
   :lmdb-error
   :make-cursor
   :make-database
   :make-environment
   :make-transaction
   :open-cursor
   :open-database
   :open-environment
   :open-p
   :put
   :reentrant-cursor-error
   :renew-transaction
   :reset-transaction
   :transaction
   :transaction-environment
   :transaction-parent
   :version-string
   :with-cursor
   :with-database
   :with-environment
   :with-transaction)
  (:documentation "The high-level LMDB interface."))

(in-package :lmdb)

;;; Library

(cffi:define-foreign-library liblmdb
  (:darwin (:or "liblmdb.dylib" "liblmdb.1.dylib"))
  (:unix  (:or "liblmdb.so" "liblmdb.so.0" "liblmdb.so.0.0.0"))
  (:win32 "liblmdb.dll")
  (t (:default "liblmdb")))

(cffi:use-foreign-library liblmdb)

;;; Constants

(defparameter *database-class* 'database)
(defparameter *environment-class* 'environment)
(defparameter *transaction-class* 'transaction)

(defparameter +permissions+ #o664
  "The Unix permissions to use for the database.")

;; Some error codes
(defparameter +enoent+ 2)
(defparameter +eacces+ 13)
(defparameter +eagain+ 11)

(defparameter *transactions* ()
  "Binds a list of all active transactions for reference in call-with-transaction")
(defparameter *transaction* nil
  "Binds the innermost active transaction")

;;; Classes

(defclass environment ()
  ((handle :accessor %handle
           :documentation "The handle on the environment poiner.
    As it is used as the pointer only, it could be bound as such, but the
    handle enables conditional finalization. (see finalize-environment)")
   (directory :reader environment-directory
              :initarg :directory
              :documentation "The directory where environment files are stored.")
   (max-databases :reader environment-max-dbs
                  :initarg :max-dbs :initarg :max-databases
                  :initform 1
                  :type integer
                  :documentation "The maximum number of named databases.")
   (max-readers :reader environment-max-readers
                :initarg :max-readers
                :initform 1
                :type integer
                :documentation "The maximum number of threads/reader slots.")
   (mapsize :reader environment-mapsize
            :initarg :mapsize
            :initform (* 1024 1024) ;; rather small
            :type integer
            :documentation "The environment mapsize specifie the database size.")
   (open-flags :reader environment-open-flags
               :initarg :open-flags
               :initform LIBLMDB:+NOTLS+
               :type integer
               :documentation "Passed to env-open.
               The default value permits multiple threads."))
  (:documentation "Environment handle."))

(defclass transaction ()
  ((handle :reader %handle
           :initarg :handle
           :documentation "The handle on the transaction pointer.")
   (env :reader transaction-environment
        :initarg :environment
        :type environment
        :documentation "The environment this transaction belongs to.")
   (parent :reader transaction-parent
           :initarg :parent
           :type (or transaction null)
           :documentation "The parent transaction, if any.")
   (databases
    :accessor transaction-databases
    :initform nil
    :type list
    :documentation "Record all databases open within the transaction.
    LMDB enforces that database remain open within the extent of
    a transaction only. In order to records this state, abort/commit/reset
    operations use this list to release the native database index."))    
  (:documentation "A transaction."))

(defclass database ()
  ((handle
    :accessor %handle
    :documentation "The DBI handle.
    The initial state is unbound, to be set/cleared by open-/close-
    and tested by with-")
   (transaction
    :reader database-transaction
    :initarg :transaction
    :type transaction
    :documentation "The transaction this database was opened in.
    Where this initial transaction commits, the database remains open
    and may be used in a subsequent transaction.")
   (name :reader database-name
         :initarg :name
         :type string
         :documentation "The database name.")
   (create :reader database-create-p
           :initarg :create
           :type boolean
           :documentation "Whether or not to create the database if it doesn't
           exist."))
  (:documentation "A database."))

(defstruct (value (:constructor %make-value))
  "A value is a generic representation of keys and values."
  (size 0 :type fixnum)
  data)

(defclass cursor ()
  ((handle :accessor %handle
           :initarg :handle
           :documentation "The pointer to the cursor object.")
   (database :reader cursor-database
             :initarg :database
             :type database
             :documentation "The database the cursor belongs to."))
  (:documentation "A cursor."))

;;; Constructors

(defun make-environment (directory &rest initargs
                                   &key
                                   (class *environment-class*)
                                   &allow-other-keys)
  "Create an environment object.

Before an environment can be used, it must be opened with @c(open-environment)."
  (declare (dynamic-extent initargs))
  (let ((instance (apply #'make-instance class
                         :directory directory
                         initargs)))
    instance))
(defmethod initialize-instance :before ((instance environment) &key class)
  ;; permit class initiarg
  (declare (ignore class)))

(defun make-transaction (environment &rest args
                                     &key (class *transaction-class*)
                                     parent
                                     &allow-other-keys)
  "Create a transaction object."
  (declare (dynamic-extent args)
           (ignore parent))
  ;; incline failed to compile
  (let ((%handle (cffi:foreign-alloc :pointer)))
    (apply #'make-instance class
           :handle %handle
           :environment environment
           args)))

(defmethod initialize-instance :before ((instance transaction) &key class)
  ;; permit class initiarg
  (declare (ignore class)))

(defun make-database (transaction name &rest initargs
                      &key (create t) (class *database-class*)
                      &allow-other-keys)
  "Create a database object.
   The initial state leaves the handle unbound (see open/close)"
  (apply #'make-instance class
         :transaction transaction
         :name name
         :create create
         initargs))
(defmethod initialize-instance :before ((instance database) &key class)
  ;; permit the class initarg
  class)

(defun convert-data (data)
  "Convert Lisp data to a format LMDB likes. Supported types are integers,
floats, booleans and strings. Returns a (size . array) pair."
  (typecase data
    (string
     (let ((octets (trivial-utf-8:string-to-utf-8-bytes data)))
       (cons (length octets) octets)))
    (vector
     (cons (length data) data))
    (integer
     (let ((octets (integer-to-octets data)))
       (cons (length octets) octets)))
    (boolean
     (cons 1 (if data 1 0)))
    (t
     (error "Invalid type."))))



(defun make-value (data)
  "Create a value object."
  (destructuring-bind (size . vector)
      (convert-data data)
    (%make-value :size size
                 :data vector)))

(defun make-cursor (database)
  "Create a cursor object.
   The initial state leaves the handle unbound."
  (make-instance 'cursor
                 :database database))

;;; Errors

(define-condition lmdb-error ()
  ()
  (:documentation "The base class of all LMDB errors."))

(define-condition lmdb-state-error (lmdb-error)
  ((object
    :initarg :object :initform (error "error object is required")
    :reader condition-object)))

(define-condition environment-error (lmdb-error)
  ((object
    :initarg :environment
    :reader condition-environment)))

(define-condition database-not-found (environment-error cell-error)
  ()
  (:documentation "database not found")
  (:report (lambda (condition stream)
             (format stream "Database not found, and did not specify :create t: ~s ~s."
                     (environment-directory (condition-environment condition))
                     (cell-error-name condition)))))

(define-condition database-maximum-count (environment-error)
  ()
  (:documentation "maximum database count reached")
  (:report (lambda (condition stream)
             (format stream "Reached maximum number of named databases: ~s."
                     (environment-directory (condition-environment condition))))))

(define-condition reentrant-cursor-error (lmdb-state-error)
  ((object
    :initarg :cursor
    :reader condition-cursor))
  (:documentation "The cursor given to open-cursor is already open.")
  (:report (lambda (condition stream)
             (format stream "Cursor is already open: ~s."
                     (condition-cursor condition)))))
(defun reentrant-cursor-error (&rest args)
  (apply #'error 'reentrant-cursor-error args))

(defun unknown-error (error-code)
  (error "Unknown error code: ~A. Result from strerror(): ~A"
         error-code
         (liblmdb:strerror error-code)))


;;; Viscera

(defgeneric handle (object)
  (:documentation "Return the handle from an environment, transaction or database.")
  (:method ((object t))
    (cffi:mem-ref (%handle object) :pointer))
  (:method ((object database))
    (cffi:mem-ref (%handle object) :uint)))

(defun release-handle (object)
  (when (slot-boundp object 'handle)
    (cffi:foreign-free (%handle object))
    (slot-makunbound object 'handle)))

(defgeneric open-p (object)
  (:method ((object t))
    (and (slot-boundp object 'handle)
         (not (cffi:null-pointer-p (cffi:mem-ref (%handle object) :pointer))))))



(defun open-environment (environment)
  "Open the environment connection.

@begin(deflist)
@term(Thread Safety)

@def(No special considerations.)

@end(deflist)"
  (with-slots (directory) environment
    (assert (uiop:directory-pathname-p directory))
    (ensure-directories-exist directory)
    (let* ((%handle (cffi:foreign-alloc :pointer)))
      (case (liblmdb:env-create %handle)
        (0 (let ((%environment (cffi:mem-ref %handle :pointer)))
             (liblmdb:env-set-maxdbs %environment (environment-max-dbs environment))
             (liblmdb:env-set-mapsize %environment (environment-mapsize environment))
             (liblmdb:env-set-maxreaders %environment (environment-max-readers environment))
             (let ((return-code
                    (liblmdb:env-open %environment
                                      (namestring directory)
                                      (environment-open-flags environment)
                                      (cffi:make-pointer +permissions+))))
               (alexandria:switch (return-code)
                                  (liblmdb:+version-mismatch+
                                   (error "Version mismatch: the client version is different from the environment version."))
                                  (liblmdb:+invalid+
                                   (error "Data corruption: the environment header files are corrupted."))
                                  (+enoent+
                                   (error "The environment directory doesn't exist."))
                                  (+eacces+
                                   (error "The user doesn't have permission to use the environment directory."))
                                  (+eagain+
                                   (error "The environment files are locked by another process."))
                                  (0
                                   ;; Success: bind the environment and configure it
                                   (setf (%handle environment) %handle)
                                   #+sbcl
                                   (sb-ext:finalize environment
                                                    #'(lambda () (finalize-environment %handle)))
                                   t)
                                  (t
                                   (unknown-error return-code))))))
        (t
         (error "Error creating environment object.")))))
  environment)

(defun environment-statistics (environment)
  "Return statistics about the environment."
  (cffi:with-foreign-object (stat '(:struct liblmdb:stat))
    (liblmdb:env-stat (handle environment)
                       stat)
    (macrolet ((slot (slot)
                 `(cffi:foreign-slot-value stat
                                           '(:struct liblmdb:stat)
                                           ',slot)))
      (list :page-size (slot liblmdb:ms-psize)
            :depth (slot liblmdb:ms-depth)
            :branch-pages (slot liblmdb:ms-branch-pages)
            :leaf-pages (slot liblmdb:ms-leaf-pages)
            :overflow-pages (slot liblmdb:ms-overflow-pages)
            :entries (slot liblmdb:ms-entries)
            ))))

(defun environment-info (environment)
  "Return information about the environment."
  (cffi:with-foreign-object (info '(:struct liblmdb:envinfo))
    (liblmdb:env-info (handle environment)
                       info)
    (macrolet ((slot (slot)
                 `(cffi:foreign-slot-value info
                                           '(:struct liblmdb:envinfo)
                                           ',slot)))
      (list :map-address (cffi:pointer-address (slot liblmdb:me-mapaddr))
            :map-size (cffi:pointer-address (slot liblmdb:me-mapsize))
            :last-page-number (cffi:pointer-address (slot liblmdb:me-last-pgno))
            :last-transaction-id (cffi:pointer-address (slot liblmdb:me-last-txnid))
            :maximum-readers (cffi:pointer-address (slot liblmdb:me-maxreaders))
            :number-of-readers (cffi:pointer-address (slot liblmdb:me-numreaders))))))


;;; transaction management

(defun begin-transaction (transaction &key (flags 0))
  "Begin the transaction.

@begin(deflist)
@term(Thread Safety)

@def(A transaction may only be used by a single thread.)

@end(deflist)"
  (with-slots (env parent) transaction
    (let ((return-code (liblmdb:txn-begin (handle env)
                                           (if parent
                                               (handle parent)
                                               (cffi:null-pointer))
                                           flags
                                           (%handle transaction))))
      (alexandria:switch (return-code)
        (0
         ;; Success
         t)
        ;; TODO rest
        (t
         (unknown-error return-code))))))

(defun release-transaction-databases (transaction)
  ;; ensure database state reflects its closure at the transaction conclusion
  #+(or)
  (loop for database in (transaction-databases transaction)
    do (release-handle database))
  (setf (transaction-databases transaction) nil))

(defun commit-transaction (transaction)
  "Commit the transaction. The transaction pointer is freed.

@begin(deflist)
@term(Thread Safety)

@def(The LMDB documentation doesn't say specifically, but assume it can only be
called by the transaction-creating thread.)

@end(deflist)"
  (let ((return-code (liblmdb:txn-commit (handle transaction))))
    (alexandria:switch (return-code :test #'=)
      (0
       ;; Success
       (setf (cffi:mem-ref (%handle transaction) :pointer) (cffi:null-pointer))
       (release-transaction-databases transaction)
       t)
      (t
       (unknown-error return-code)))))

(defun abort-transaction (transaction)
  "Abort the transaction. The transaction pointer is freed.

@begin(deflist)
@term(Thread Safety)

@def(The LMDB documentation doesn't say specifically, but assume it can only be
called by the transaction-creating thread.)

@end(deflist)"
  (liblmdb:txn-abort (handle transaction))
  (setf (cffi:mem-ref (%handle transaction) :pointer) (cffi:null-pointer))
  (release-transaction-databases transaction))

(defun renew-transaction (transaction)
  "Renew the transaction.

@begin(deflist)
@term(Thread Safety)

@def(A transaction may only be used by a single thread.)

@end(deflist)"
  (with-slots (env parent) transaction
    (let ((return-code (liblmdb:txn-renew (handle transaction))))
      (alexandria:switch (return-code)
        (0
         ;; Success
         t)
        ;; TODO rest
        (t
         (unknown-error return-code))))))

(defun reset-transaction (transaction)
  "Reset the transaction.

@begin(deflist)
@term(Thread Safety)

@def(A transaction may only be used by a single thread.)

@end(deflist)"
  (liblmdb:txn-reset (handle transaction))
  (release-transaction-databases transaction))

(defgeneric enter-transaction (transaction disposition &key flags)
  (:method ((transaction lmdb:transaction) (disposition (eql :begin)) &rest args)
    (apply #'begin-transaction transaction args))
  (:method ((transaction lmdb:transaction) (disposition (eql :renew)) &rest args)
    (declare (ignore args))
    (renew-transaction transaction)))

(defgeneric leave-transaction (transaction disposition)
  (:method ((transaction transaction) (disposition (eql :abort)))
    (abort-transaction transaction))
  (:method ((transaction transaction) (disposition (eql :commit)))
    (commit-transaction transaction))
  (:method ((transaction transaction) (disposition (eql :reset)))
    (reset-transaction transaction)))


;;; database management

(defgeneric open-database (database)
  (:documentation "Open a database.
Bind the dbi handle and call dbi-open to set it.
@begin(deflist)
@term(Thread Safety)

@def(A transaction that opens a database must finish (either commit or abort)
before another transaction may open it. Multiple concurrent transactions cannot
open the same database.)

@end(deflist)")
  (:method ((database database))
  (with-slots (transaction name create) database
    (when (find database (transaction-databases transaction))
      (error "open-database: reentrant invocation: ~s ~s." database transaction))
    (unless (open-p transaction)
      (error "open-database: transaction not active: ~s ~s." database transaction))
    (unless (slot-boundp database 'handle)
      ;; dbi-open claims to be idempotent, but anyway
      (let* ((%handle (cffi:foreign-alloc :uint))
             (return-code (liblmdb:dbi-open (handle transaction)
                                            name
                                            (logior 0
                                                    (if create
                                                        liblmdb:+create+
                                                        0))
                                            %handle)))
        (alexandria:switch (return-code)
          (0
           ;; Success
           (push database (transaction-databases transaction))
           (setf (%handle database) %handle))
          (liblmdb:+notfound+
           (error 'database-not-found :name name :environment (transaction-environment transaction)))
          (liblmdb:+dbs-full+
           (error 'database-maximum-count :name name :environment (transaction-environment transaction)))
          (t
           (unknown-error return-code))))))
  (values database
          (handle database))))

(defmethod print-object ((object database) stream)
  (let ((*print-pretty* nil))
    (print-unreadable-object (object stream :identity t :type t)
      (format stream "~s" (if (slot-boundp object 'name)
                              (slot-value object 'name)
                              "?")))))


(defgeneric drop-database  (database &key drop)
  (:method ((database database) &key (delete 0))
    (liblmdb:drop (handle transaction) (handle graph-db) delete)))

;;; cursor operations

(defun open-cursor (cursor)
  "Open a cursor."
  (when (slot-boundp cursor 'handle)
    (reentrant-cursor-error :cursor cursor))
  (with-slots (database) cursor
    (with-slots (transaction) database
      (let* ((%handle (cffi:foreign-alloc :pointer))
             (return-code (liblmdb:cursor-open (handle transaction)
                                               (handle database)
                                               %handle)))
        (alexandria:switch (return-code)
          (0
           ;; Success
           (setf (%handle cursor) %handle))
          (22
           (error "Invalid parameter."))
          (t
           (unknown-error return-code))))))
  cursor)

;;; Querying

(defmacro with-val ((raw-value data) &body body)
  (alexandria:with-gensyms (value-struct array)
    `(let* ((,value-struct (make-value ,data))
            (,raw-value (cffi:foreign-alloc '(:struct liblmdb:val)))
            (,array (cffi:foreign-alloc :unsigned-char
                                        :count (value-size ,value-struct))))
       (setf (cffi:foreign-slot-value ,raw-value
                                      '(:struct liblmdb:val)
                                      'liblmdb:mv-size)
             (cffi:make-pointer (value-size ,value-struct)))

       (loop for elem across (value-data ,value-struct)
             for i from 0 to (1- (length (value-data ,value-struct)))
             do
                (setf (cffi:mem-aref ,array :unsigned-char i)
                      elem))
       (setf (cffi:foreign-slot-value ,raw-value
                                      '(:struct liblmdb:val)
                                      'liblmdb:mv-data)
             ,array)
       ,@body)))

(defmacro with-empty-value ((value) &body body)
  `(cffi:with-foreign-object (,value '(:struct liblmdb:val))
     ,@body))

(defun raw-value-to-vector (raw-value)
  (let* ((size (cffi:pointer-address
                (cffi:foreign-slot-value raw-value
                                         '(:struct liblmdb:val)
                                         'liblmdb:mv-size)))
         (array (cffi:foreign-slot-value raw-value
                                         '(:struct liblmdb:val)
                                         'liblmdb:mv-data))
         (vec (make-array size
                          :element-type '(unsigned-byte 8))))
    (loop for i from 0 to (1- size) do
      (setf (elt vec i)
            (cffi:mem-aref array :unsigned-char i)))
    vec))

(defun get (database key)
  "Get a value from the database."
  (with-slots (transaction) database
    (with-val (raw-key key)
      (with-empty-value (raw-value)
        (let ((return-code (liblmdb:get (handle transaction)
                                         (handle database)
                                         raw-key
                                         raw-value)))
          (alexandria:switch (return-code)
            (0
             ;; Success
             (values (raw-value-to-vector raw-value) t))
            (liblmdb:+notfound+
             (values nil nil))
            (t
             (unknown-error return-code))))))))

(defun put (database key value)
  "Add a value to the database."
  (with-slots (transaction) database
    (with-val (raw-key key)
      (with-val (raw-val value)
        (let ((return-code (liblmdb:put (handle transaction)
                                         (handle database)
                                         raw-key
                                         raw-val
                                         0)))
          (alexandria:switch (return-code)
            (0
             ;; Success
             t)
            (t
             (unknown-error return-code)))))))
  value)

(defun del (database key &optional data)
  "Delete this key from the database. Returns @c(t) if the key was found,
@c(nil) otherwise."
  (with-slots (transaction) database
    (with-val (raw-key key)
      (let ((return-code (liblmdb:del (handle transaction)
                                       (handle database)
                                       raw-key
                                       (if data
                                           data
                                           (cffi:null-pointer)))))
        (alexandria:switch (return-code)
          (0
           ;; Success
           t)
          (liblmdb:+notfound+
           nil)
          (+eacces+
           (error "An attempt was made to delete a key in a read-only transaction."))
          (t
           (unknown-error return-code)))))))

(defun cursor-get (cursor operation)
  "Extract data using a cursor.

The @cl:param(operation) argument specifies the operation."
  (let ((op (case operation
              (:first :+first+)
              (:current :+current+)
              (:last :+last+)
              (:next :+next+)
              (:prev :+prev+))))
    (with-empty-value (raw-key)
      (with-empty-value (raw-value)
        (let ((return-code (liblmdb:cursor-get (handle cursor)
                                                raw-key
                                                raw-value
                                                op)))
          (alexandria:switch (return-code)
            (0
             ;; Success
             (values (raw-value-to-vector raw-key)
                     (raw-value-to-vector raw-value)))
            (liblmdb:+notfound+
             (values nil nil))
            (t
             (unknown-error return-code))))))))

(defun cursor-get-with (cursor operation continuation)
  "Extract data using a cursor.

The @cl:param(operation) argument specifies the operation."
  (declare (dynamic-extent continuation))
  (let ((op (case operation
              (:first :+first+)
              (:current :+current+)
              (:last :+last+)
              (:next :+next+)
              (:prev :+prev+)
              (:set-range :+set-range+))))
    (with-empty-value (raw-key)
      (with-empty-value (raw-value)
        (let ((return-code (liblmdb:cursor-get (handle cursor)
                                                raw-key
                                                raw-value
                                                op)))
          (alexandria:switch (return-code)
            (0
             ;; Success
             (funcall continuation raw-key raw-value))
            (liblmdb:+notfound+
             (values nil nil))
            (t
             (unknown-error return-code))))))))

(defun get-with (database key operator)
  "Get a value from the database."
  (with-slots (transaction) database
    (with-val (raw-key key)
      (with-empty-value (raw-value)
        (let ((return-code (liblmdb:get (handle transaction)
                                         (handle database)
                                         raw-key
                                         raw-value)))
          (alexandria:switch (return-code)
            (0
             ;; Success
             (funcall operator raw-key raw-value))
            (liblmdb:+notfound+
             (values nil nil))
            (t
             (unknown-error return-code))))))))

;;; Destructors

(defun finalize-environment (%handle)
  "When an environment instance is no longer reachable, examine its
 lmdb environment handle. Iff that is still open, close it.
 Finally, free the handle"
  (let ((%env (cffi:mem-ref %handle :pointer)))
    (unless (cffi:null-pointer-p %env)
      (liblmdb:env-close %env)
      ;; to be sure
      (setf (cffi:mem-ref %handle :pointer) (cffi:null-pointer)))
    (cffi:foreign-free %handle)))

(defun close-environment (environment)
  "Close the environment connection and free the memory.

@begin(deflist)
@term(Thread Safety)

@def(Only a single thread may call this function. All environment-dependent
objects, such as transactions and databases, must be closed before calling this
function. Attempts to use those objects are closing the environment will result
in a segmentation fault.)

@end(deflist)"
  (when (slot-boundp environment 'handle)
    ;; similar to finalize, but do not free the handle.
    (let ((%env (handle environment)))
      (unless (cffi:null-pointer-p %env)
        (liblmdb:env-close %env)
        (setf (cffi:mem-ref (%handle environment) :pointer) (cffi:null-pointer))))
    ;;!! this eliminates the reference, but leaves the handle allocated to be
    ;; available to the finalize-environment operator
    (slot-makunbound environment 'handle))
  t)

(defun close-database (database)
  "Close the database.

@begin(deflist)
@term(Thread Safety)

@def(Despair.

From the LMDB documentation,

@quote(This call is not mutex protected. Handles should only be closed by a
single thread, and only if no other threads are going to reference the database
handle or one of its cursors any further. Do not close a handle if an existing
transaction has modified its database. Doing so can cause misbehavior from
database corruption to errors like MDB_BAD_VALSIZE (since the DB name is
gone).))

@end(deflist)"
  (with-slots (transaction name create) database
    (liblmdb:dbi-close (handle (transaction-environment transaction))
                       (handle database))
    (release-handle database)
    (setf (transaction-databases transaction)
          (remove database (transaction-databases transaction)))
    t))

(defun close-cursor (cursor)
  "Close a cursor."
  (liblmdb:cursor-close (handle cursor))
  (cffi:foreign-free (%handle cursor))
  (slot-makunbound cursor 'handle)
  t)


;;; Macros

(defun call-with-open-environment (op environment)
  (declare (dynamic-extent op))
  (cond ((slot-boundp environment 'handle)
         (funcall op))
        (t
         (open-environment environment)
         (unwind-protect
             (funcall op)
           (close-environment environment)))))

(defmacro with-environment ((environment) &body body)
  "Execute the @cl:param(body) in a context which ensures that the @cl:param(environment) is open.
 If that is already the case, do not change the state.
 If open was necessary, close the environment upon conclusion."
  (let ((op (gensym "with-environment-body-")))
    `(flet ((,op () ,@body))
       (declare (dynamic-extent #',op))
       (call-with-open-environment #',op ,environment))))

(defun call-ensuring-open-environment (op environment)
  (declare (dynamic-extent op))
  (unless (slot-boundp environment 'handle)
    (open-environment environment))
  (funcall op))

(defmacro ensure-open-environment ((environment) &body body)
  "Execute the @cl:param(body) in a context which ensures that the @cl:param(environment) is open.
 If that is already the case, do not change the state.
 In any case, leave the environment open upon conclusion"
  (let ((op (gensym "ensure-environment-body-")))
    `(flet ((,op () ,@body))
       (declare (dynamic-extent #',op))
       (call-ensuring-open-environment #',op ,environment))))


(defun call-with-transaction (op transaction
                                 &key
                                 (normal-disposition :reset)
                                 (error-disposition :abort)
                                 (initial-disposition :begin)
                                 (flags () fs-p))
  "If the transaction is already established, just call the operator.
 Otherwise, wrap that call with bindings for the current and nested
 transaction, instantiate the transaction and track completion when
 closing it."
  (declare (dynamic-extent op))
  (cond ((find transaction *transactions*)
         (funcall op transaction))
        (t
         (let ((status nil)
               (*transactions* (cons transaction *transactions*))
               (*transaction* transaction)
               (flags-args (when fs-p `(:flags ,flags))))
           (apply #'enter-transaction transaction initial-disposition flags-args)
           (unwind-protect (multiple-value-prog1 (funcall op transaction)
                             (leave-transaction transaction normal-disposition)
                             (setf status normal-disposition))
             (unless status
               (leave-transaction transaction error-disposition)))))))

(defmacro with-transaction ((transaction &rest options
                                         &key normal-disposition
                                         error-disposition
                                         initial-disposition
                                         flags)
                            &body body)
  (declare (ignore normal-disposition error-disposition initial-disposition flags))
  (let ((op (gensym))
        (transaction-variable (if (consp transaction) (first transaction) transaction))
        (transaction-form (if (consp transaction)
                              (if (consp (second transaction))
                                  (second transaction)
                                  `(make-transaction ,@(rest transaction)))
                              transaction)))
    `(flet ((,op (,transaction-variable)
              (declare (ignorable ,transaction-variable))
              ,@body))
       (declare (dynamic-extent #',op))
       (call-with-transaction #',op ,transaction-form ,@options))))

(defun call-with-open-database (op database)
  (declare (dynamic-extent op))
  (cond ((slot-boundp database 'handle)
         (funcall op))
        (t
         (open-database database)
         (unwind-protect
             (funcall op)
           (close-database database)))))

(defmacro with-database ((database) &body body)
  "Execute the body in a context which ensures that the database is open.
 If that is already the case, do not change the state.
 If open was necessary, close the database upon conclusion."
  (let ((op (gensym "with-database-body-")))
    `(flet ((,op () ,@body))
       (declare (dynamic-extent #',op))
       (call-with-open-database #',op ,database))))

(defun ensure-open-database (database)
  (unless (slot-boundp database 'handle)
    (open-database database))
  database)

(defmacro with-cursor ((cursor) &body body)
  "Execute the body and close the cursor."
  `(progn
     (open-cursor ,cursor)
     (unwind-protect
          (progn ,@body)
       (close-cursor ,cursor))))

(defmacro do-pairs ((db key value) &body body)
  "Iterate over every key/value pair in the database."
  (let ((cur (gensym)))
    `(let ((,cur (make-cursor ,db)))
       (with-cursor (,cur)
         (multiple-value-bind (,key ,value)
             (cursor-get ,cur :first)
           (loop while ,key do
             ,@body
             (multiple-value-bind (tk tv)
                 (cursor-get ,cur :next)
               (setf ,key tk
                     ,value tv))))))))

(defmacro do-pairs-with ((db key value operator) &body body)
  "Iterate over every key/value pair in the database."
  (let ((cur (gensym)))
    `(let ((,cur (make-cursor ,db)))
       (with-cursor (,cur)
         (multiple-value-bind (,key ,value)
             (cursor-get-with ,cur :first ,operator)
           (loop while ,key do
             ,@body
             (multiple-value-bind (tk tv)
                 (cursor-get-with ,cur :next ,operator)
               (setf ,key tk
                     ,value tv))))))))

;;; Utilities

;;; this serves to compute the key length.
;;; if
(defun integer-to-octets (bignum)
  "Return a byte vector padded to standard integer sizes when they fit,
 that is for 4 and 8 byte words, or variable above that.
 The standard case is to ensure compatibility with values stored as
 native word and long integers in other languages, _but_ in little-endian form
 only."
  (let* ((n-bits (integer-length bignum))
         (n-bytes-exact (ceiling n-bits 8))
         (n-bytes (cond ((> n-bytes-exact 8) n-bytes-exact)
                        ((> n-bytes-exact 4) 8)
                        (t 4)))
         (octet-vec (make-array n-bytes :element-type '(unsigned-byte 8))))
    (declare (type (simple-array (unsigned-byte 8)) octet-vec))
    (loop
       :for index  :from (1- n-bytes) :downto 0
       :do (setf (aref octet-vec index) (ldb (byte 8 (* index 8)) bignum))
       :finally (return octet-vec))))

(defun version-string ()
  "Return the version string."
  (format nil "~D.~D.~D"
          liblmdb:+version-major+
          liblmdb:+version-minor+
          liblmdb:+version-patch+))
